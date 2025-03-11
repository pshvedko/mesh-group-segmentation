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

type Putter interface {
	Put(context.Context, *sqlx.DB) error
}

type Loader[T Putter] interface {
	Load(context.Context, int, chan<- T) (int, error)
}

type Driver[T Putter] interface {
	Loader[T]
	Save(context.Context, T) error
}

type Drive[T Putter] struct {
	Loader[T]
	*sqlx.DB
}

func (s *Drive[T]) Save(ctx context.Context, item T) error {
	return item.Put(ctx, s.DB)
}

func NewDriver[T Putter](loader Loader[T], db *sqlx.DB) (Driver[T], error) {
	return &Drive[T]{
		Loader: loader,
		DB:     db,
	}, nil
}

type Getter[T Putter] interface {
	Get(context.Context, string, chan<- T) (int, error)
}

type Decoder[T Putter] func(context.Context, io.Reader, chan<- T) (int, error)

func (f Decoder[T]) Decode(ctx context.Context, r io.Reader, c chan<- T) (int, error) {
	return f(ctx, r, c)
}

type Get[T Putter] struct {
	Decoder[T]
	http.Client
	UserAgent string
}

func (g *Get[T]) Get(ctx context.Context, URL string, items chan<- T) (int, error) {
	req, err := g.NewRequestWithContext(ctx, URL)
	if err != nil {
		return 0, err
	}
	res, err := g.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = res.Body.Close() }()
	return g.Decode(ctx, res.Body, items)
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

func NewGetter[T Putter](agent string, timeout time.Duration, decoder Decoder[T]) (Getter[T], error) {
	return &Get[T]{
		Client:    http.Client{Timeout: timeout},
		Decoder:   decoder,
		UserAgent: agent,
	}, nil
}

type Pager interface {
	Page(int) (string, error)
}

type Load[T Putter] struct {
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

func (p *Page) Page(size int) (string, error) {
	u := p.URL
	q := u.Query()
	q.Set(p.Offset, strconv.Itoa(p.Start))
	q.Set(p.Limit, strconv.Itoa(size))
	u.RawQuery = q.Encode()
	p.Start += size
	return u.String(), nil
}

func NewPager(URL url.URL, offset string, limit string) Pager {
	return &Page{
		URL:    URL,
		Offset: offset,
		Limit:  limit,
		Start:  0,
	}
}

func (l *Load[T]) Load(ctx context.Context, size int, items chan<- T) (int, error) {
	page, err := l.Page(size)
	if err != nil {
		return 0, err
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-time.After(l.Duration + time.Until(l.Time)):
		l.Time = time.Now()
	}
	return l.Get(ctx, page, items)
}

func NewLoader[T Putter](interval time.Duration, URL url.URL, offset, limit string, getter Getter[T]) (Loader[T], error) {
	return &Load[T]{
		Pager:    NewPager(URL, offset, limit),
		Getter:   getter,
		Duration: interval,
	}, nil
}

type Import[T Putter] struct {
	Driver[T]
	Size int
}

func (i *Import[T]) Import(ctx context.Context) error {
	c := make(chan T)
	e := make(chan error, 1)

	defer func() {
		<-c
		<-e
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func(ctx context.Context, c chan<- T, e chan<- error) {
		defer close(c)
		defer close(e)
		for {
			n, err := i.Load(ctx, i.Size, c)
			if err != nil || n == 0 {
				e <- err
				break
			}
		}
	}(ctx, c, e)

	for item := range c {
		err := i.Save(ctx, item)
		if err != nil {
			return err
		}
	}

	return <-e
}

type Importer[T Putter] interface {
	Import(context.Context) error
}

func New[T Putter](driver Driver[T], size int) (Importer[T], error) {
	return &Import[T]{
		Driver: driver,
		Size:   size,
	}, nil
}

func NewImporter[T Putter](loader Loader[T], db *sqlx.DB, size int) (Importer[T], error) {
	driver, err := NewDriver(loader, db)
	if err != nil {
		return nil, nil
	}
	return New(driver, size)
}
