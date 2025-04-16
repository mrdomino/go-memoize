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
//
// This package favors simplicity and generality over maximal deduplication of
// work — in many common cases, there will be more calls to the underlying
// function than strictly necessary. For instance, this package does nothing
// about concurrency: multiple parallel calls with the same inputs will all go
// to the underlying function until one of them returns. As well, in the vein
// of generality, a default key function is provided for all proto messages,
// even though [proto serialization is not canonical] so the same messages will
// sometimes have different keys.
//
// For best results, memoizing should be done “close to” requests — so e.g. on
// the client rather than the server side, where it is more likely that all of
// the relevant fields to the request will be accounted for. This also has the
// advantage that clients will talk directly to your memcached instance rather
// than having to go through a gRPC server.
//
// The main API exported by this package is [Wrap]. This function is well-suited
// to gRPC servers, and is in fact shaped the same way as a gRPC RPC modulo
// the target. [Wrap] uses a package-default memoizer constructed via [New]; if
// a custom [Memoizer] is desired, then [WrapWithMemoizer] may be used instead.
//
// A client-side [grpc.UnaryInterceptor] is also exported as [Intercept], which
// corresponds to [Wrap]; or [InterceptWithMemoizer], which corresponds to
// [WrapWithMemoizer].
//
// [proto serialization is not canonical]: https://protobuf.dev/programming-guides/serialization-not-canonical/
package memoize

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mrdomino/go-memoize/keyer"
	"google.golang.org/protobuf/proto"
)

// Func is the basic unit that is memoized by this package.
// It is shaped like a gRPC RPC call but without the target.
type Func[Req, Res proto.Message] func(context.Context, Req) (Res, error)

// Wrap returns a memoized version of the [Func] f using the [Cache] c.
// The type should normally be inferred; for further discussion, see the comment
// for [WrapWithMemoizer].
func Wrap[T any, Req proto.Message, Res protoMessage[T]](
	c Cache, f Func[Req, Res], opts ...Option,
) Func[Req, Res] {
	return WrapWithMemoizer(New(c, opts...), f)
}

// WrapWithMemoizer returns a memoized version of f using the passed [Memoizer].
// The type should normally be inferred; for example, given:
//
//	var myFunc func(ctx context.Context, *pb.RpcRequest) (*pb.RpcReply, error)
//
// MyFunc can be memoized by simply saying:
//
//	memo := Wrap(myMemcache, myFunc)
//
// The returned memo function will have the [Func] type:
//
//	Func[*pb.RpcRequest, *pb.RpcReply]
//
// which corresponds to the raw type:
//
//	func(context.Context, *pb.RpcRequest) (*pb.RpcReply, error)
//
// This is the same type as that of a gRPC server function with request type
// RpcRequest and reply type RpcReply. As such, it should be relatively simple
// to use these wrappers in gRPC server contexts, like so:
//
//	type Server struct {
//	    pb.UnimplementedFooServer
//	    memoSayHello memoize.Func[*pb.HelloRequest, *pb.HelloReply]
//	}
//
//	func New(cache memoize.Cache) *Server {
//	    s := &Server{}
//	    s.memoSayHello = Wrap(cache, func(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
//	        return s.SayHello_Raw(ctx, in)
//	    })
//
//	func (s *Server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
//	    return s.memoSayHello(ctx, in)
//	}
//
//	func (s *Server) SayHello_Raw(_ context.Context, in *pb.HelloReqest) (*pb.HelloReply, error) {
//	    // do the actual work
//	}
//
// The protoMessage[T] in the type of WrapWithMemoizer is an implementation
// detail; it should be read “Res is a [proto.Message] and also a *T for some T.”
// It exists to allow a zero value of type T to be created without having to use
// reflection.
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
		if err := getProto(m, key, res); err == nil {
			return
		} else if !errors.Is(err, ErrCacheMiss) {
			errorf("getProto(%q): %w", key, err)
		}

		res, err = f(ctx, req)
		if err != nil {
			return res, err
		}

		if err := addProto(
			m, key, res,
			expiration(ctx, req, res),
			flags(ctx, req, res),
		); err != nil && !errors.Is(err, ErrNotStored) {
			errorf("addProto(%q): %w", key, err)
		}
		return
	}
}

// Option customizes a memoizer’s behavior.
type Option func(*builder)

// WithCustomKeyer allows supplying a completely custom [keyer.Keyer] for cache
// keys.
func WithCustomKeyer(k keyer.Keyer) Option {
	return func(b *builder) {
		b.keyer = k
	}
}

// WithCustomKeyFunc allows supplying a raw function to produce cache keys.
func WithCustomKeyFunc(f func(context.Context, proto.Message) (string, error)) Option {
	return WithCustomKeyer(keyer.KeyFunc(f))
}

// WithHashKeyerOpts appends to the options passed to the default
// [keyer.HashKeyer] that is constructed if no other [keyer.Keyer] or
// [keyer.KeyFunc] is passed.
func WithHashKeyerOpts(opts ...keyer.HashKeyerOption) Option {
	return func(b *builder) {
		b.hashKeyerOpts = append(b.hashKeyerOpts, opts...)
	}
}

// WithErrorHandler allows setting a custom [Errer] for this memoizer.
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
//  1. A [Cache] to store and retreive values.
//  2. A [keyer.Keyer] to generate keys from inputs.
//
// A Memoizer may additionally implement the following optional interfaces to
// opt in to additional functionality:
//
//  1. [HasErrer] to receive non-fatal errors that may occur in memoization but
//     that do not affect function outputs.
//  2. [Expirer] to set expiration times on cache items.
//  3. [Flagger] to set custom flags on cache items.
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

// Expirer returns the expiration time for the cache entry corresponding to
// the passed context, and request/result messages, in the format expected by
// [Item]. That is: “the cache expiration time, in seconds: either a relative
// time from now (up to 1 month), or an absolute Unix epoch time. Zero means
// […] no expiration time.”
type Expirer interface {
	Expiration(_ context.Context, req, res proto.Message) int32
}

// Flagger sets custom flags on cache entries, corresponding to the Flags field
// on [Item].
type Flagger interface {
	Flags(_ context.Context, req, res proto.Message) uint32
}

// New constructs a new package-default memoizer with the given [Option] slice.
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

// memoizer is the default [Memoizer] for this package.
type memoizer struct {
	Cache
	keyer.Keyer
	errer Errer
	ttl   time.Duration
	flags uint32
}

// builder accumulates build options for [*memoizer].
type builder struct {
	keyer keyer.Keyer
	errer Errer
	ttl   time.Duration
	flags uint32

	hashKeyerOpts []keyer.HashKeyerOption
}

func (m *memoizer) Expiration(_ context.Context, _, _ proto.Message) int32 {
	if m.ttl == 0 {
		return 0
	}
	return int32(time.Now().Add(m.ttl).Unix())
}

func (m *memoizer) Flags(_ context.Context, _, _ proto.Message) uint32 {
	return m.flags
}

func (m *memoizer) IsErrerEnabled() bool {
	return m.errer != nil
}

func (m *memoizer) Error(err error) {
	m.errer.Error(err)
}

// protoMessage is an implementation detail; it allows us to instantiate a new
// T as an output value without using reflection.
type protoMessage[T any] interface {
	proto.Message
	*T
}
