package sap_segmentation

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
)

type Item interface {
	Put(context.Context, *sqlx.DB) error
}

type Loader[T Item] interface {
	Load(context.Context, int) ([]T, error)
	Shift(int)
}

type Driver[T Item] interface {
	Loader[T]
	Save(context.Context, T) error
}

type Drive[T Item] struct {
	Loader[T]
	*sqlx.DB
}

func (s *Drive[T]) Save(ctx context.Context, item T) error {
	return item.Put(ctx, s.DB)
}

func NewDriver[T Item](loader Loader[T], db *sqlx.DB) (Driver[T], error) {
	return &Drive[T]{
		Loader: loader,
		DB:     db,
	}, nil
}

type Getter[T Item] interface {
	Get(context.Context, string) ([]T, error)
}

type Decoder[T Item] func(io.Reader) ([]T, error)

func (f Decoder[T]) Decode(r io.ReadCloser) ([]T, error) {
	return f(r)
}

type Get[T Item] struct {
	Decoder[T]
	http.Client
	UserAgent string
}

func (g *Get[T]) Get(ctx context.Context, URL string) ([]T, error) {
	req, err := g.NewRequestWithContext(ctx, URL)
	if err != nil {
		return nil, err
	}
	res, err := g.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	return g.Decode(res.Body)
}

func (g *Get[T]) NewRequestWithContext(ctx context.Context, URL string) (*http.Request, error) {
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, URL, &bytes.Buffer{})
	if err != nil {
		return nil, err
	}
	r.Header.Set("Connection", "keep-alive")
	r.Header.Set("User-Agent", g.UserAgent)
	return r, nil

}

func NewGetter[T Item](agent string, timeout time.Duration, decoder Decoder[T]) (Getter[T], error) {
	return &Get[T]{
		Client:    http.Client{Timeout: timeout},
		Decoder:   decoder,
		UserAgent: agent,
	}, nil
}

type Pager interface {
	Page(int) (string, error)
	Shift(int)
}

type Load[T Item] struct {
	Getter[T]
	time.Duration
	time.Time
	Pager
}

type Page struct {
	url.URL
	Offset string
	Limit  string
	Start  int
}

func (p *Page) Shift(shift int) {
	p.Start += shift
}

func (p *Page) Page(size int) (string, error) {
	u := p.URL
	q := u.Query()
	q.Set(p.Offset, strconv.Itoa(p.Start))
	q.Set(p.Limit, strconv.Itoa(size))
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (l *Load[T]) Load(ctx context.Context, size int) ([]T, error) {
	page, err := l.Page(size)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(l.Duration + time.Until(l.Time)):
		l.Time = time.Now()
	}
	return l.Get(ctx, page)
}

func NewLoader[T Item](URL url.URL, offset, limit string, interval time.Duration, getter Getter[T]) (Loader[T], error) {
	return &Load[T]{
		Pager:    NewPager(URL, offset, limit),
		Getter:   getter,
		Duration: interval,
	}, nil
}

func NewPager(URL url.URL, offset string, limit string) Pager {
	return &Page{
		URL:    URL,
		Offset: offset,
		Limit:  limit,
		Start:  0,
	}
}

type Import[T Item] struct {
	Driver[T]
	Size int
}

func (i *Import[T]) Import(ctx context.Context) error {
	for {
		items, err := i.Load(ctx, i.Size)
		if err != nil || len(items) == 0 {
			return err
		}
		for _, item := range items {
			err = i.Save(ctx, item)
			if err != nil {
				return err
			}
			i.Shift(1)
		}
	}
}

type Importer[T Item] interface {
	Import(context.Context) error
}

func New[T Item](driver Driver[T], size int) (Importer[T], error) {
	return &Import[T]{
		Driver: driver,
		Size:   size,
	}, nil
}

func NewImporter[T Item](loader Loader[T], db *sqlx.DB, size int) (Importer[T], error) {
	driver, err := NewDriver(loader, db)
	if err != nil {
		return nil, nil
	}
	return New(driver, size)
}
