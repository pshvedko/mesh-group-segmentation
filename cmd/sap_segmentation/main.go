package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/kelseyhightower/envconfig"
	"github.com/spf13/cobra"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/samber/lo"
	"github.com/samber/slog-multi"

	"github.com/pshvedko/sap_segmentation"
	"github.com/pshvedko/sap_segmentation/internal/config"
	"github.com/pshvedko/sap_segmentation/internal/logfile"
	"github.com/pshvedko/sap_segmentation/model"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	ModulePrefix = "sap_segmentation"
	LogPath      = "log"
)

func main() {
	var cfg config.Config

	err := envconfig.Process(ModulePrefix, &cfg)
	if err != nil {
		_ = envconfig.Usage(ModulePrefix, &cfg)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c := &cobra.Command{
		Use:  ModulePrefix,
		Long: "MESH GROUP Golang test assignment",
		PersistentPreRunE: func(*cobra.Command, []string) error {
			return prepare(ctx, cfg)
		},
		RunE: func(*cobra.Command, []string) error {
			return run(ctx, cfg)
		},
	}

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

	err = c.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func prepare(_ context.Context, cfg config.Config) error {
	out, err := logfile.New(LogPath, ModulePrefix, 24*time.Hour*time.Duration(cfg.LogCleanupMaxAge))
	switch {
	case err != nil:
	case out != nil:
		slog.SetDefault(
			slog.New(
				slogmulti.Fanout(
					slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}),
					slog.NewJSONHandler(out, &slog.HandlerOptions{}),
				),
			),
		)
	}
	return err
}

func run(ctx context.Context, cfg config.Config) error {
	db, err := sqlx.Open("pgx", cfg.DB.DataSourceName("postgres"))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	getter, err := sap_segmentation.NewGetter(cfg.Conn.UserAgent, cfg.Conn.Timeout,
		func(r io.Reader) ([]model.Segmentation, error) {
			var items []model.Segmentation
			err := json.NewDecoder(r).Decode(&items)
			if err != nil {
				return nil, err
			}
			return items, nil
		},
	)
	if err != nil {
		return err
	}

	loader, err := sap_segmentation.NewLoader(cfg.Conn.URL(), "p_offset", "p_limit", cfg.Conn.Interval, getter)
	if err != nil {
		return err
	}

	importer, err := sap_segmentation.NewImporter(loader, db, cfg.ImportBatchSize)
	if err != nil {
		return err
	}

	return importer.Import(ctx)
}

//go:embed migration
var migrationFS embed.FS

func setup(ctx context.Context, cfg config.Config, down bool) error {
	db, err := sqlx.Open("pgx", cfg.DB.DataSourceName("postgres"))
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
