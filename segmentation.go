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

type Putter[T any] interface {
	Put(context.Context, *sqlx.DB) (T, error)
}

type Driver[T Putter[T]] interface {
	Loader[T]
	Save(context.Context, T) (T, error)
	UseLoader(...func(Loader[T]) Loader[T])
}

type Drive[T Putter[T]] struct {
	Loader[T]
	*sqlx.DB
}

func (d *Drive[T]) UseLoader(wrappers ...func(Loader[T]) Loader[T]) {
	for _, wrapper := range wrappers {
		d.Loader = wrapper(d.Loader)
	}
}

func (d *Drive[T]) Save(ctx context.Context, item T) (T, error) {
	return item.Put(ctx, d.DB)
}

func NewDriver[T Putter[T]](db *sqlx.DB, loader Loader[T]) (Driver[T], error) {
	return &Drive[T]{
		Loader: loader,
		DB:     db,
	}, nil
}

type Decoder[T Putter[T]] func(context.Context, io.Reader, chan<- T) (int, error)

func (f Decoder[T]) Decode(ctx context.Context, r io.Reader, c chan<- T) (int, error) {
	return f(ctx, r, c)
}

type Getter[T Putter[T]] interface {
	Get(context.Context, url.URL, chan<- T) (int, error)
}

type Get[T Putter[T]] struct {
	Decoder[T]
	http.Client
	UserAgent string
}

func (g *Get[T]) Get(ctx context.Context, URL url.URL, items chan<- T) (int, error) {
	req, err := g.NewRequestWithContext(ctx, URL.String())
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

func NewGetter[T Putter[T]](agent string, timeout time.Duration, decoder Decoder[T]) (Getter[T], error) {
	return &Get[T]{
		Client:    http.Client{Timeout: timeout},
		Decoder:   decoder,
		UserAgent: agent,
	}, nil
}

type Pager interface {
	Page(int) (url.URL, error)
}

type Page struct {
	url.URL
	Offset string
	Limit  string
	Start  int
}

func (p *Page) Page(size int) (url.URL, error) {
	u := p.URL
	q := u.Query()
	q.Set(p.Offset, strconv.Itoa(p.Start))
	q.Set(p.Limit, strconv.Itoa(size))
	u.RawQuery = q.Encode()
	p.Start += size
	return u, nil
}

func NewPager(URL url.URL, offset string, limit string) Pager {
	return &Page{
		URL:    URL,
		Offset: offset,
		Limit:  limit,
		Start:  0,
	}
}

type Loader[T Putter[T]] interface {
	Load(context.Context, int, chan<- T) (int, error)
	UseGetter(...func(Getter[T]) Getter[T])
}

type Load[T Putter[T]] struct {
	Getter[T]
	time.Duration
	time.Time
	Pager
}

func (l *Load[T]) UseGetter(wrappers ...func(Getter[T]) Getter[T]) {
	for _, wrapper := range wrappers {
		l.Getter = wrapper(l.Getter)
	}
}

func (l *Load[T]) Load(ctx context.Context, size int, items chan<- T) (int, error) {
	URL, err := l.Page(size)
	if err != nil {
		return 0, err
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-time.After(l.Duration + time.Until(l.Time)):
		l.Time = time.Now()
	}
	return l.Get(ctx, URL, items)
}

func NewLoader[T Putter[T]](interval time.Duration, URL url.URL, offset, limit string, getter Getter[T]) (Loader[T], error) {
	return &Load[T]{
		Pager:    NewPager(URL, offset, limit),
		Getter:   getter,
		Duration: interval,
	}, nil
}

type Importer[T Putter[T]] interface {
	Import(context.Context) error
	WithLoader(...func(Loader[T]) Loader[T]) Importer[T]
	WithDriver(...func(Driver[T]) Driver[T]) Importer[T]
	WithGetter(...func(Getter[T]) Getter[T]) Importer[T]
	UseLoader(...func(Loader[T]) Loader[T])
	UseDriver(...func(Driver[T]) Driver[T])
	UseGetter(...func(Getter[T]) Getter[T])
}

type Import[T Putter[T]] struct {
	Driver[T]
	Size int
}

func (i Import[T]) WithLoader(wrappers ...func(Loader[T]) Loader[T]) Importer[T] {
	i.UseLoader(wrappers...)
	return &i
}

func (i Import[T]) WithDriver(wrappers ...func(Driver[T]) Driver[T]) Importer[T] {
	i.UseDriver(wrappers...)
	return &i
}

func (i Import[T]) WithGetter(wrappers ...func(Getter[T]) Getter[T]) Importer[T] {
	i.UseGetter(wrappers...)
	return &i
}

func (i *Import[T]) UseDriver(wrappers ...func(Driver[T]) Driver[T]) {
	for _, wrapper := range wrappers {
		i.Driver = wrapper(i.Driver)
	}
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
		_, err := i.Save(ctx, item)
		if err != nil {
			return err
		}
	}

	return <-e
}

func New[T Putter[T]](size int, driver Driver[T]) (Importer[T], error) {
	return &Import[T]{
		Driver: driver,
		Size:   size,
	}, nil
}

func NewImporter[T Putter[T]](size int, db *sqlx.DB, loader Loader[T]) (Importer[T], error) {
	driver, err := NewDriver(db, loader)
	if err != nil {
		return nil, nil
	}
	return New(size, driver)
}
