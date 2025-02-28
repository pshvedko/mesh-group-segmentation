package sap_segmentation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"

	"github.com/jmoiron/sqlx"
	"github.com/kelseyhightower/envconfig"

	"github.com/pshvedko/sap_segmentation/internal/config"
)

const MaxObject = 30

type Object struct {
	ID int `json:"id"`
}

func (o Object) Put(context.Context, *sqlx.DB) error {
	_, err := fmt.Print(o)
	return err
}

type Handler struct{}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a, ok := r.Header["Authorization"]
	if !ok || len(a) != 1 || a[0] != "Basic MTox" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	offset, _ := strconv.Atoi(r.FormValue("offset"))
	if offset >= MaxObject {
		offset = MaxObject
	}

	limit, _ := strconv.Atoi(r.FormValue("limit"))
	limit = min(limit, MaxObject-offset)
	objects := make([]Object, 0, limit)

	for limit > 0 {
		objects = append(objects, Object{ID: offset})
		offset++
		limit--
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(objects)
}

type G[T Item] struct {
	Getter[T]
}

func (g G[T]) Get(ctx context.Context, URL string) ([]T, error) {
	items, err := g.Getter.Get(ctx, URL)
	switch err {
	case nil:
		slog.Info(URL, "len", len(items))
	default:
		slog.Error(URL, "err", err)
	}
	return items, err
}

type D[T Item] struct {
	Driver[T]
}

func (d D[T]) Load(ctx context.Context, items []T) (int, error) {
	return d.Driver.Load(ctx, items)
}

func (d D[T]) Save(ctx context.Context, item T) error {
	err := d.Driver.Save(ctx, item)
	switch err {
	case nil:
		slog.Info("+++", "item", item)
	default:
		slog.Error("---", "item", item, "err", err)
	}
	return err
}

func ExampleNewImporter() {
	s := httptest.NewServer(Handler{})
	defer s.Close()

	_ = os.Setenv("TEST_CONN_URI", s.URL)
	_ = os.Setenv("TEST_CONN_AUTH_LOGIN_PWD", "1:1")
	_ = os.Setenv("TEST_CONN_INTERVAL", "50ms")
	_ = os.Setenv("TEST_IMPORT_BATCH_SIZE", "8")

	var cfg config.Config
	err := envconfig.Process("TEST", &cfg)
	if err != nil {
		fmt.Println(err)
		return
	}

	getter, err := NewGetter(cfg.Conn.UserAgent, cfg.Conn.Timeout, func(r io.Reader) ([]Object, error) {
		var items []Object
		err := json.NewDecoder(r).Decode(&items)
		if err != nil {
			return nil, err
		}
		return items, nil
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	loader, err := NewLoader(cfg.Conn.URL(), "offset", "limit", cfg.Conn.Interval, G[Object]{Getter: getter})
	if err != nil {
		fmt.Println(err)
		return
	}

	driver, err := NewDriver(loader, &sqlx.DB{})
	if err != nil {
		fmt.Println(err)
	}

	importer, err := New(D[Object]{Driver: Driver[Object](driver)}, cfg.ImportBatchSize)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = importer.Import(context.TODO())
	if err != nil {
		fmt.Println(err)
		return
	}

	// Output:
	//
	// {0}{1}{2}{3}{4}{5}{6}{7}{8}{9}{10}{11}{12}{13}{14}{15}{16}{17}{18}{19}{20}{21}{22}{23}{24}{25}{26}{27}{28}{29}
}
