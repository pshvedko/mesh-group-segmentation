package sap_segmentation

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"

	"github.com/jmoiron/sqlx"
	"github.com/kelseyhightower/envconfig"

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

type Handler struct {
	N int
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a, ok := r.Header["Authorization"]
	if !ok || len(a) != 1 || a[0] != "Basic MTox" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	offset, _ := strconv.Atoi(r.FormValue("offset"))
	if offset >= h.N {
		offset = h.N
	}

	limit, _ := strconv.Atoi(r.FormValue("limit"))
	limit = min(limit, h.N-offset)
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

func ExampleNewImporter() {
	h := Handler{N: 30}
	s := httptest.NewServer(&h)
	defer s.Close()

	_ = os.Setenv("TEST_CONN_URI", s.URL)
	_ = os.Setenv("TEST_CONN_AUTH_LOGIN_PWD", "1:1")
	_ = os.Setenv("TEST_CONN_INTERVAL", "50ms")
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
