package sap_segmentation

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
)

type G[T Putter[T]] struct {
	Getter[T]
}

func (g G[T]) Get(ctx context.Context, URL url.URL, items chan<- T) (int, error) {
	n, err := g.Getter.Get(ctx, URL, items)
	switch err {
	case nil:
		slog.Info(URL.Redacted(), "count", n)
	default:
		slog.Error(URL.Redacted(), "count", n, "err", err)

	}
	return n, err
}

func LogGetter[T Putter[T]](getter Getter[T]) Getter[T] {
	return G[T]{Getter: getter}
}

type D[T Putter[T]] struct {
	Driver[T]
}

func (d D[T]) Save(ctx context.Context, item T) (T, error) {
	item, err := d.Driver.Save(ctx, item)
	switch err {
	case nil:
		slog.Debug(fmt.Sprintf("%+v", item))
	default:
		slog.Error(fmt.Sprintf("%+v", item), "err", err)
	}
	return item, err
}

func LogDriver[T Putter[T]](driver Driver[T]) Driver[T] {
	return D[T]{Driver: driver}
}
