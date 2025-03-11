package stream

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

type Handler[T any] struct {
	Offset string
	Limit  string
	Size   int
	New    func(offset int) T
}

func (h Handler[T]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	offset, _ := strconv.Atoi(r.FormValue(h.Offset))
	if offset >= h.Size {
		offset = h.Size
	}

	limit, _ := strconv.Atoi(r.FormValue(h.Limit))
	limit = min(limit, h.Size-offset)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	_, err := fmt.Fprint(w, json.Delim('['))
	if err != nil {
		return
	}

	j := json.NewEncoder(w)
	for limit > 0 {
		err = j.Encode(h.New(offset))
		if err != nil {
			return
		}
		if limit > 1 {
			_, err = fmt.Fprint(w, json.Delim(','))
			if err != nil {
				return
			}
		}
		offset++
		limit--
	}

	_, _ = fmt.Fprint(w, json.Delim(']'))
}

func (h Handler[T]) WithOffset(offset string) Handler[T] {
	h.Offset = offset
	return h
}

func (h Handler[T]) WithLimit(limit string) Handler[T] {
	h.Limit = limit
	return h
}

func NewHandler[T any](size int, newer func(offset int) T) Handler[T] {
	return Handler[T]{
		Offset: "offset",
		Limit:  "limit",
		Size:   size,
		New:    newer,
	}
}

type AuthorizationHandler[T any] struct {
	Basic string
	Handler[T]
}

func (h AuthorizationHandler[T]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a, ok := r.Header["Authorization"]
	if ok && a[0] == h.Basic {
		h.Handler.ServeHTTP(w, r)
	} else {
		w.WriteHeader(http.StatusUnauthorized)
	}
}

func NewHandlerWithAuthorization[T any](size int, basic string, newer func(offset int) T) AuthorizationHandler[T] {
	return AuthorizationHandler[T]{
		Basic:   basic,
		Handler: NewHandler(size, newer),
	}
}
