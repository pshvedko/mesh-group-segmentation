package stream

import (
	"context"
	"encoding/json"
	"io"
)

func Decode[T any](ctx context.Context, r io.Reader, c chan<- T) (n int, err error) {
	j := json.NewDecoder(r)
	// read open bracket
	_, err = j.Token()
	if err != nil {
		return
	}
	// while the array contains values
	for j.More() {
		var o T
		// decode an array value
		err = j.Decode(&o)
		if err != nil {
			return
		}
		select {
		case c <- o:
			n++
		case <-ctx.Done():
			err = ctx.Err()
			return
		}
	}
	// read closing bracket
	_, err = j.Token()
	return
}
