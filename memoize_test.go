/*
Copyright 2025 Steven Dee

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package memoize

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/mrdomino/go-memoize/keyer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func calledFunc() (*int, func(context.Context, *wrapperspb.UInt64Value) (*wrapperspb.StringValue, error)) {
	var calls int
	return &calls, func(ctx context.Context, req *wrapperspb.UInt64Value) (*wrapperspb.StringValue, error) {
		calls += 1
		return wrapperspb.String(strconv.FormatUint(req.GetValue(), 10)), nil
	}
}

func TestWrap(t *testing.T) {
	ctx := t.Context()
	cache := NewLocalCache()
	calls, origFunc := calledFunc()
	var res *wrapperspb.StringValue
	var err error

	memo := Wrap(cache, origFunc)
	res, err = memo(ctx, wrapperspb.UInt64(1))
	require.NoError(t, err)
	assert.Equal(t, 1, *calls)
	assert.Equal(t, "1", res.GetValue())
	res, err = memo(ctx, wrapperspb.UInt64(1))
	require.NoError(t, err)
	assert.Equal(t, 1, *calls)
	assert.Equal(t, "1", res.GetValue())
	res, err = memo(ctx, wrapperspb.UInt64(2))
	require.NoError(t, err)
	assert.Equal(t, 2, *calls)
	assert.Equal(t, "2", res.GetValue())
	res, err = memo(ctx, wrapperspb.UInt64(1))
	require.NoError(t, err)
	assert.Equal(t, 2, *calls)
	assert.Equal(t, "1", res.GetValue())
}

func TestWrap_TTL(t *testing.T) {
	ctx := t.Context()
	cache := NewLocalCache()
	calls, origFunc := calledFunc()
	var err error

	memo := Wrap(cache, origFunc, WithTTL(time.Hour))
	_, err = memo(ctx, wrapperspb.UInt64(1))
	require.NoError(t, err)
	cache.AdvanceTime(time.Hour + 1)
	_, err = memo(ctx, wrapperspb.UInt64(1))
	require.NoError(t, err)
	assert.Equal(t, 2, *calls)
}

func TestWrap_KeyError(t *testing.T) {
	ctx := t.Context()
	cache := NewLocalCache()
	calls, origFunc := calledFunc()
	var errs []error
	var res *wrapperspb.StringValue
	var err error

	memo := Wrap(cache, origFunc,
		WithErrorFunc(func(err error) {
			errs = append(errs, err)
		}),
		WithCustomKeyFunc(func(ctx context.Context, m proto.Message) (string, error) {
			return "", errors.ErrUnsupported
		}),
	)

	res, err = memo(ctx, wrapperspb.UInt64(1))
	require.NoError(t, err)
	assert.Equal(t, 1, *calls)
	assert.Equal(t, "1", res.GetValue())
	assert.Len(t, errs, 1)
	assert.ErrorIs(t, errs[0], errors.ErrUnsupported)

	errs = nil
	res, err = memo(ctx, wrapperspb.UInt64(1))
	require.NoError(t, err)
	assert.Equal(t, 2, *calls)
	assert.Equal(t, "1", res.GetValue())
	assert.Len(t, errs, 1)
	assert.ErrorIs(t, errs[0], errors.ErrUnsupported)
}

func TestWrap_Errors(t *testing.T) {
	tests := []struct {
		name     string
		getErr   bool
		addErr   bool
		wantErrs []string
	}{
		{
			name:     "cache.Get error is reported",
			getErr:   true,
			wantErrs: []string{"getProto"},
		},
		{
			name:     "cache.Add error is reported",
			addErr:   true,
			wantErrs: []string{"addProto"},
		},
		{
			name:     "add & get errors are reported",
			addErr:   true,
			getErr:   true,
			wantErrs: []string{"getProto", "addProto"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			origFunc := func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				return nil, nil
			}
			var errs []error
			cache := NewLocalCache()
			if tt.getErr {
				cache.Down.Store(true)
			}
			if tt.addErr {
				cache.Full.Store(true)
			}
			memo := Wrap(cache, origFunc, WithErrorFunc(func(err error) {
				errs = append(errs, err)
			}))
			_, err := memo(t.Context(), nil)
			require.NoError(t, err)
			require.Len(t, errs, len(tt.wantErrs))
			for i, err := range errs {
				assert.Contains(t, err.Error(), tt.wantErrs[i])
			}
		})
	}
}

func TestWrap_KeyFunc(t *testing.T) {
	cache := NewLocalCache()
	origFunc := func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return nil, nil
	}
	memo := Wrap(cache, origFunc, WithCustomKeyFunc(func(ctx context.Context, m proto.Message) (string, error) {
		return "custom key", nil
	}))
	memo(t.Context(), wrapperspb.String(""))
	assert.Len(t, cache.data, 1)
	item, ok := cache.data["custom key"]
	require.Truef(t, ok, "custom key not in map")
	require.Equal(t, []uint8(nil), item.Value)
}

func TestWrap_MultipleTypes(t *testing.T) {
	tests := []struct {
		name       string
		typePrefix bool
	}{
		{
			name:       "no type prefix breaks",
			typePrefix: false,
		},
		{
			name:       "type prefix works",
			typePrefix: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			func1 := func(context.Context, *wrapperspb.BoolValue) (*wrapperspb.StringValue, error) {
				return wrapperspb.String("Hello, world"), nil
			}
			func2 := func(context.Context, *wrapperspb.UInt64Value) (*wrapperspb.BoolValue, error) {
				return wrapperspb.Bool(true), nil
			}
			cache := NewLocalCache()
			m := New(cache, WithHashKeyerOpts(keyer.WithTypePrefix(tt.typePrefix)))
			memo1 := WrapWithMemoizer(m, func1)
			memo2 := WrapWithMemoizer(m, func2)
			ctx := t.Context()
			res, err := memo1(ctx, wrapperspb.Bool(true))
			require.NoError(t, err)
			assert.Equal(t, "Hello, world", res.GetValue())
			res2, err := memo2(ctx, wrapperspb.UInt64(1))
			require.NoError(t, err)
			if tt.typePrefix {
				assert.Equal(t, true, res2.GetValue())
			} else {
				// XXX
				assert.Equal(t, false, res2.GetValue())
			}
		})
	}
}

func TestWrap_Concurrency(t *testing.T) {
	// This basically just shows that we do not do any in-progress-operation
	// tracking.
	ctx := t.Context()
	seq1 := make(chan struct{})
	seq2 := make(chan struct{})
	origFunc := func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		<-seq1
		seq2 <- struct{}{}
		return wrapperspb.String(""), nil
	}
	cache := NewLocalCache()
	memo := Wrap(cache, origFunc)
	go memo(ctx, nil)
	seq1 <- struct{}{}
	for range 8 {
		go memo(ctx, nil)
	}
	for range 8 {
		seq1 <- struct{}{}
	}
	go func() {
		memo(ctx, nil)
		close(seq2)
	}()
	seq1 <- struct{}{}
	var calls int
	for range seq2 {
		calls += 1
	}
	assert.Equal(t, calls, 10)
	memo(ctx, nil)
	assert.Equal(t, calls, 10)
}

func BenchmarkWrap(b *testing.B) {
	myFunc := func(_ context.Context, n *wrapperspb.UInt64Value) (*wrapperspb.UInt64Value, error) {
		return n, nil
	}
	cache := NewLocalCache()
	memo := Wrap(cache, myFunc)
	ctx := b.Context()
	n := wrapperspb.UInt64(1)
	memo(ctx, n)
	for b.Loop() {
		memo(ctx, n)
	}
}
