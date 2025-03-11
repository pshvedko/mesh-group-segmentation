package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/kelseyhightower/envconfig"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/samber/lo"
	"github.com/samber/slog-multi"

	"github.com/pshvedko/sap_segmentation"
	"github.com/pshvedko/sap_segmentation/internal/config"
	"github.com/pshvedko/sap_segmentation/internal/logfile"
	"github.com/pshvedko/sap_segmentation/internal/stream"
	"github.com/pshvedko/sap_segmentation/model"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	ModulePrefix = "sap_segmentation"
	LogPath      = "log"
)

type Level struct {
	p *slog.Level
}

func (l Level) String() string {
	return l.p.String()
}

func (l Level) Set(s string) error {
	switch strings.ToUpper(s[:1]) {
	case "E":
		s = "ERROR"
	case "W":
		s = "WARN"
	case "I":
		s = "INFO"
	case "D":
		s = "DEBUG"
	}
	return l.p.UnmarshalText([]byte(s))
}

func (l Level) Type() string {
	return "level"
}

func NewLogLevel(p *slog.Level, v slog.Level) pflag.Value {
	*p = v
	return Level{p: p}
}

func main() {
	var cfg config.Config

	err := envconfig.Process(ModulePrefix, &cfg)
	if err != nil {
		_ = envconfig.Usage(ModulePrefix, &cfg)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var usage bool
	var level slog.Level

	c := &cobra.Command{
		Use:  ModulePrefix,
		Long: "MESH GROUP Golang test assignment",
		PersistentPreRunE: func(*cobra.Command, []string) error {
			return prepare(ctx, cfg, level)
		},
		RunE: func(*cobra.Command, []string) error {
			if usage {
				return envconfig.Usage(ModulePrefix, &cfg)
			}
			return run(ctx, cfg)
		},
	}

	c.PersistentFlags().VarP(NewLogLevel(&level, slog.LevelInfo), "level", "l", "level")
	c.PersistentFlags().BoolVarP(&usage, "usage", "u", false, "usage")
	c.PersistentFlags().IPVar(&cfg.DB.Host, "host", cfg.DB.Host, "host")
	c.PersistentFlags().IntVar(&cfg.DB.Port, "port", cfg.DB.Port, "port")
	c.PersistentFlags().StringVar(&cfg.DB.User, "user", cfg.DB.User, "user")
	c.PersistentFlags().Var(config.Hide(&cfg.DB.Password), "password", "password")

	var down bool

	m := &cobra.Command{
		Use:   "migrate",
		Short: "Migrate database schema",
		RunE: func(*cobra.Command, []string) error {
			return setup(ctx, cfg, down)
		},
	}

	m.Flags().BoolVar(&down, "down", false, "downgrade")
	c.AddCommand(m)

	var addr string
	var size int

	w := &cobra.Command{
		Use:   "demo",
		Short: "Demo service",
		RunE: func(*cobra.Command, []string) error {
			return demo(ctx, addr, size)
		},
	}

	w.Flags().StringVarP(&addr, "address", "a", ":8080", "address")
	w.Flags().IntVarP(&size, "count", "n", 1000, "count")
	c.AddCommand(w)

	err = c.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func demo(ctx context.Context, addr string, size int) error {
	h := http.NewServeMux()
	h.HandleFunc("/demo", func(w http.ResponseWriter, r *http.Request) {
		offset, _ := strconv.Atoi(r.FormValue("p_offset"))
		if offset >= size {
			offset = size
		}
		limit, _ := strconv.Atoi(r.FormValue("p_limit"))
		limit = min(limit, size-offset)
		objects := make([]model.Segmentation, 0, limit)
		for limit > 0 {
			objects = append(objects, model.Segmentation{
				AddressSapId: strconv.Itoa(offset % 10),
				AdrSegment:   strconv.Itoa(offset),
				SegmentId:    int64(offset),
			})
			offset++
			limit--
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(objects)
	})
	w := http.Server{
		Addr:        addr,
		Handler:     h,
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	// Make sure the program doesn't exit and waits instead for Shutdown to return.
	g := sync.WaitGroup{}
	defer g.Wait()
	context.AfterFunc(ctx, func() {
		g.Add(1)
		_ = w.Shutdown(context.TODO())
		g.Done()
	})
	return w.ListenAndServe()
}

func prepare(_ context.Context, cfg config.Config, level slog.Level) error {
	out, err := logfile.New(LogPath, ModulePrefix, 24*time.Hour*time.Duration(cfg.LogCleanupMaxAge))
	switch {
	case err != nil:
	case out != nil:
		slog.SetDefault(
			slog.New(
				slogmulti.Fanout(
					slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}),
					slog.NewJSONHandler(out, &slog.HandlerOptions{Level: level}),
				),
			),
		)
	}
	return err
}

func run(ctx context.Context, cfg config.Config) error {
	db, err := sqlx.Open("pgx", cfg.DB.DSN("postgres"))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	getter, err := sap_segmentation.NewGetter(cfg.Conn.UserAgent, cfg.Conn.Timeout, stream.Decode[model.Segmentation])
	if err != nil {
		return err
	}

	loader, err := sap_segmentation.NewLoader(cfg.Conn.Interval, cfg.Conn.URL(), "p_offset", "p_limit", getter)
	if err != nil {
		return err
	}

	importer, err := sap_segmentation.NewImporter(cfg.ImportBatchSize, db, loader)
	if err != nil {
		return err
	}

	return importer.
		WithGetter(sap_segmentation.LogGetter[model.Segmentation]).
		WithDriver(sap_segmentation.LogDriver[model.Segmentation]).
		ImportWithBuffer(ctx, cfg.ImportBatchSize)
}

//go:embed migration
var migrationFS embed.FS

func setup(ctx context.Context, cfg config.Config, down bool) error {
	db, err := sqlx.Open("pgx", cfg.DB.DSN("postgres"))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	driver, err := postgres.WithConnection(ctx, conn, &postgres.Config{})
	if err != nil {
		return err
	}

	source, err := iofs.New(migrationFS, "migration")
	if err != nil {
		return err
	}

	migration, err := migrate.NewWithInstance("embed", source, "segmentation", driver)
	if err != nil {
		return err
	}

	err = lo.Ternary(down, migration.Down, migration.Up)()
	switch {
	case errors.Is(err, migrate.ErrNoChange):
		return nil
	default:
		return err
	}
}
