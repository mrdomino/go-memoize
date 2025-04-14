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

// Package memoize implements simple function memoization for protobuf messages
// using a memcache-like cache.
package memoize

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mrdomino/go-memoize/keyer"
	"google.golang.org/protobuf/proto"
)

// Func is the basic unit that is memoized by this package. It is shaped like a
// gRPC RPC call.
type Func[Req, Res proto.Message] func(context.Context, Req) (Res, error)

// Wrap returns a memoized version of f, using the Cache c to store outputs for
// previously seen inputs.
func Wrap[T any, Req proto.Message, Res protoMessage[T]](
	c Cache, f Func[Req, Res], opts ...Option,
) Func[Req, Res] {
	return WrapWithMemoizer(New(c, opts...), f)
}

// WrapWithMemoizer returns a memoized version of f using the passed Memoizer.
func WrapWithMemoizer[T any, Req proto.Message, Res protoMessage[T]](
	m Memoizer, f Func[Req, Res],
) Func[Req, Res] {
	expiration := func(_ context.Context, _, _ proto.Message) int32 {
		return 0
	}
	flags := func(_ context.Context, _, _ proto.Message) uint32 {
		return 0
	}
	errorf := func(string, ...any) {
	}

	if expirer, ok := m.(Expirer); ok {
		expiration = expirer.Expiration
	}
	if flagger, ok := m.(Flagger); ok {
		flags = flagger.Flags
	}
	var errer Errer
	if hasErrer, ok := m.(HasErrer); ok && hasErrer.IsErrerEnabled() {
		errer = hasErrer
	} else {
		errer = ErrorHandler
	}
	if errer != nil {
		preamble := fmt.Sprintf("%T(%p).", m, m)
		errorf = func(format string, args ...any) {
			errer.Error(fmt.Errorf(preamble+format, args...))
		}
	}

	return func(ctx context.Context, req Req) (res Res, _ error) {
		key, err := m.Key(ctx, req)
		if err != nil {
			errorf("Key(%v): %w", req, err)
			return f(ctx, req)
		}
		res = new(T)
		if err := GetProto(m, key, res); err == nil {
			return
		} else if !errors.Is(err, ErrCacheMiss) {
			errorf("GetProto(%q): %w", key, err)
		}

		res, err = f(ctx, req)
		if err != nil {
			return res, err
		}

		if err := AddProto(
			m, key, res,
			expiration(ctx, req, res),
			flags(ctx, req, res),
		); err != nil {
			errorf("AddProto(%q): %w", key, err)
		}
		return
	}
}

// Option customizes a memoizer's behavior.
type Option func(*builder)

// WithCustomKeyer allows supplying a completely custom Keyer for cache keys.
func WithCustomKeyer(k keyer.Keyer) Option {
	return func(b *builder) {
		b.keyer = k
	}
}

// WithCustomKeyFunc allows supplying a raw function to produce cache keys.
func WithCustomKeyFunc(f func(context.Context, proto.Message) (string, error)) Option {
	return WithCustomKeyer(keyer.KeyFunc(f))
}

// WithHashKeyerOpts allows overriding the options passed to the default
// HashKeyer that is constructed if no other Keyer or KeyFunc is passed.
func WithHashKeyerOpts(opts ...keyer.HashKeyerOption) Option {
	return func(b *builder) {
		b.hashKeyerOpts = append(b.hashKeyerOpts, opts...)
	}
}

// WithErrorHandler allows setting a custom error handler for this memoizer.
func WithErrorHandler(errer Errer) Option {
	return func(b *builder) {
		b.errer = errer
	}
}

// WithErrorFunc allows passing a raw function to receive memoization errors.
func WithErrorFunc(f func(error)) Option {
	return WithErrorHandler(ErrFunc(f))
}

// WithTTL allows setting a flat TTL to be set on all cache entries from this
// memoizer.
func WithTTL(ttl time.Duration) Option {
	return func(b *builder) {
		b.ttl = ttl
	}
}

// WithFlags allows setting constant flags to be set on all cache entries from
// this memoizer.
func WithFlags(flags uint32) Option {
	return func(b *builder) {
		b.flags = flags
	}
}

// Memoizer is the interface required to provide memoization for functions in
// this API. It consists of:
//
//  1. A Cache to store and retreive values.
//  2. A Keyer to generate keys from inputs.
//
// A Memoizer may additionally implement the following optional interfaces to
// opt in to additional functionality:
//
//  1. HasErrer to receive non-fatal errors that may occur in memoization but
//     that do not affect function outputs.
//  2. Expirer to set expiration times on cache items.
//  3. Flagger to set custom flags on cache items.
type Memoizer interface {
	Cache
	keyer.Keyer
}

// HasErrer is an Errer that is optionally enabled or not.
type HasErrer interface {
	Errer
	// IsErrerEnabled returns true if this Errer is live and receiving errors;
	// this allows e.g. a type to implement Errer but have it be conditional
	// whether any individual instance should receive errors.
	IsErrerEnabled() bool
}

// Expirer allows a Memoizer to set an expiration time on its cache entries
// based on any of the context, the input, or the output. The format is that
// used by memcache.Item; it supports either relative times in seconds up to
// one month, or an absolute Unix timestamp in seconds, with 0 for unlimited.
type Expirer interface {
	Expiration(_ context.Context, req, res proto.Message) int32
}

// Flagger allows a Memoizer to set custom flags on its cache entries based on
// any of the content, the input, or the output.
type Flagger interface {
	Flags(_ context.Context, req, res proto.Message) uint32
}

// New constructs a new memoizer with the given options.
func New(c Cache, opts ...Option) *memoizer {
	b := &builder{}
	for _, opt := range opts {
		opt(b)
	}
	if b.keyer == nil {
		b.keyer = keyer.NewHashKeyer(b.hashKeyerOpts...)
	}
	return &memoizer{
		Cache: c,
		Keyer: b.keyer,
		errer: b.errer,
		ttl:   b.ttl,
		flags: b.flags,
	}
}

var (
	_ Memoizer = (*memoizer)(nil)
	_ Expirer  = (*memoizer)(nil)
	_ Flagger  = (*memoizer)(nil)
	_ HasErrer = (*memoizer)(nil)
)

// memoizer is the default Memoizer for this package.
type memoizer struct {
	Cache
	keyer.Keyer
	errer Errer
	ttl   time.Duration
	flags uint32
}

// builder accumulates build for a memoizer.
type builder struct {
	keyer keyer.Keyer
	errer Errer
	ttl   time.Duration
	flags uint32

	hashKeyerOpts []keyer.HashKeyerOption
}

// Expiration implements Expirer.Exipration for memoizer.
func (m *memoizer) Expiration(_ context.Context, _, _ proto.Message) int32 {
	if m.ttl == 0 {
		return 0
	}
	return int32(time.Now().Add(m.ttl).Unix())
}

// Flags implements Flagger.Flags for memoizer.
func (m *memoizer) Flags(_ context.Context, _, _ proto.Message) uint32 {
	return m.flags
}

// IsErrerEnabled implements HasErrer.IsErrerEnabled for memoizer.
func (m *memoizer) IsErrerEnabled() bool {
	return m.errer != nil
}

// Error implements Errer.Error for memoizer.
func (m *memoizer) Error(err error) {
	m.errer.Error(err)
}

// protoMessage is an implementation detail; it allows us to instantiate a new
// T as an output value without using reflection.
type protoMessage[T any] interface {
	proto.Message
	*T
}
