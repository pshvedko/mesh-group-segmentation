package sap_segmentation

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/kelseyhightower/envconfig"
	"log/slog"
	"net/http/httptest"
	"os"

	"github.com/pshvedko/sap_segmentation/internal/config"
	"github.com/pshvedko/sap_segmentation/internal/stream"
)

type Object struct {
	ID int `json:"id"`
}

func (o Object) Put(context.Context, *sqlx.DB) (Object, error) {
	_, err := fmt.Print(o)
	return o, err
}

func ExampleNewImporter() {
	h := stream.NewHandlerWithAuthorization(30, "Basic MTox", func(offset int) Object {
		return Object{ID: offset}
	})
	s := httptest.NewServer(h)
	defer s.Close()

	_ = os.Setenv("TEST_CONN_URI", s.URL)
	_ = os.Setenv("TEST_CONN_AUTH_LOGIN_PWD", "1:1")
	_ = os.Setenv("TEST_CONN_INTERVAL", "500ms")
	_ = os.Setenv("TEST_IMPORT_BATCH_SIZE", "8")

	slog.SetLogLoggerLevel(slog.LevelDebug)

	var cfg config.Config
	err := envconfig.Process("TEST", &cfg)
	if err != nil {
		fmt.Println(err)
		return
	}

	getter, err := NewGetter(cfg.Conn.UserAgent, cfg.Conn.Timeout, stream.Decode[Object])
	if err != nil {
		fmt.Println(err)
		return
	}

	loader, err := NewLoader(cfg.Conn.Interval, cfg.Conn.URL(), "offset", "limit", LogGetter(getter))
	if err != nil {
		fmt.Println(err)
		return
	}

	driver, err := NewDriver(&sqlx.DB{}, loader)
	if err != nil {
		fmt.Println(err)
	}

	importer, err := New(cfg.ImportBatchSize, LogDriver(driver))
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
