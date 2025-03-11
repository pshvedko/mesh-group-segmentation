package sap_segmentation

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
)

type G[T Putter] struct {
	Getter[T]
}

func (g G[T]) Get(ctx context.Context, URL url.URL, items chan<- T) (int, error) {
	slog.Info(URL.Redacted())
	return g.Getter.Get(ctx, URL, items)
}

func LogGetter[T Putter](getter Getter[T]) Getter[T] {
	return G[T]{Getter: getter}
}

type D[T Putter] struct {
	Driver[T]
}

func (d D[T]) Save(ctx context.Context, item T) error {
	err := d.Driver.Save(ctx, item)
	switch err {
	case nil:
		slog.Info(fmt.Sprintf("%+v", item))
	default:
		slog.Error(fmt.Sprintf("%+v", item), "err", err)
	}
	return err
}

func LogDriver[T Putter](driver Driver[T]) Driver[T] {
	return D[T]{Driver: driver}
}
