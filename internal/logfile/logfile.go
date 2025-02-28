package logfile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const LogTimeLayout = "20060102150405"

func New(path, prefix string, age time.Duration) (io.WriteCloser, error) {
	if path == "" || prefix == "" {
		return nil, nil
	}

	err := os.MkdirAll(path, 0755)
	if err != nil {
		return nil, err
	}

	one := filepath.Join(path, fmt.Sprintf("%s.log", prefix))
	two := filepath.Join(path, fmt.Sprintf("%s.%s.log", prefix, time.Now().UTC().Format(LogTimeLayout)))

	err = os.Rename(one, two)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	err = Rotate(path, fmt.Sprint(prefix, " %s log"), age)
	if err != nil {
		return nil, err
	}

	return os.OpenFile(one, os.O_WRONLY|os.O_CREATE, 0644)
}

func Rotate(path, format string, age time.Duration) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		var (
			date string
			n    int
			t    time.Time
		)

		stamp := strings.ReplaceAll(entry.Name(), ".", " ")
		n, err = fmt.Sscanf(stamp, format, &date)
		if err != nil || n != 1 {
			continue
		}

		t, err = time.Parse(LogTimeLayout, date)
		if err != nil {
			continue
		}

		if time.Since(t) < age {
			continue
		}

		err = os.Remove(filepath.Join(path, entry.Name()))
		if err != nil {
			return err
		}
	}

	return nil
}
