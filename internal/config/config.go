package config

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/spf13/pflag"
)

var ErrInvalidUserInfo = errors.New("invalid user info")

type UserPassword struct {
	*url.Userinfo
}

func (u *UserPassword) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		u.Userinfo = nil

		return nil
	}

	n := bytes.IndexRune(text, ':')
	if n == -1 {
		return ErrInvalidUserInfo
	}

	u.Userinfo = url.UserPassword(string(text[:n]), string(text[n+1:]))

	return nil
}

type DataBase struct {
	Host     net.IP `default:"127.0.0.1" desc:"host"`
	Port     int    `default:"5432" desc:"port"`
	Name     string `default:"mesh_group" desc:"name"`
	User     string `default:"postgres" desc:"user"`
	Password string `default:"postgres" desc:"password"`
}

func (db DataBase) DSN(scheme string) string {
	return fmt.Sprint(&url.URL{
		Scheme: scheme,
		User:   url.UserPassword(db.User, db.Password),
		Host:   db.Host.String(),
		Path:   db.Name,
	})
}

type Source struct {
	URI          url.URL       `default:"http://bsm.api.iql.ru/ords/bsm/segmentation/get_segmentation" desc:"uri"`
	AuthLoginPwd UserPassword  `default:"4Dfddf5:jKlljHGH" split_words:"true" desc:"auth login password"`
	UserAgent    string        `default:"spacecount-test" split_words:"true" desc:"user agent"`
	Timeout      time.Duration `default:"5s" desc:"timeout"`
	Interval     time.Duration `default:"1500ms" desc:"interval"`
}

func (s Source) URL() url.URL {
	URL := s.URI
	URL.User = s.AuthLoginPwd.Userinfo
	return URL
}

type Config struct {
	DB               DataBase
	Conn             Source
	ImportBatchSize  int `default:"50" split_words:"true" desc:"import batch size"`
	LogCleanupMaxAge int `default:"7" split_words:"true" desc:"log cleanup max age"`
}

type Hidden struct {
	s *string
}

func (h Hidden) String() string {
	return "***"
}

func (h Hidden) Set(s string) error {
	*h.s = s
	return nil
}

func (h Hidden) Type() string {
	return "string"
}

func Hide(h *string) pflag.Value {
	return Hidden{s: h}
}
