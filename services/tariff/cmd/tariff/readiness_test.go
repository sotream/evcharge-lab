package main

import (
	"context"
	"errors"
	"testing"
)

func TestCacheAwareReadiness(t *testing.T) {
	down := func(context.Context) error { return errors.New("postgres down") }
	up := func(context.Context) error { return nil }
	for name, tc := range map[string]struct {
		pg      func(context.Context) error
		warm    bool
		wantErr bool
	}{
		"postgres up, cold cache":   {up, false, false},
		"postgres up, warm cache":   {up, true, false},
		"postgres down, warm cache": {down, true, false},
		"postgres down, cold cache": {down, false, true},
	} {
		err := cacheAwareReadiness(tc.pg, func() bool { return tc.warm })(context.Background())
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err=%v wantErr=%v", name, err, tc.wantErr)
		}
	}
}
