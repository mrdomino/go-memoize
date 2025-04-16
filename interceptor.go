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
	"fmt"

	"github.com/mrdomino/go-memoize/keyer"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Intercept is a [grpc.UnaryClientInterceptor] that memoizes gRPC clients.
// It uses the passed [Cache] with a package-default memoizer (see [New].)
//
// Note that the [keyer.HashKeyer] will be constructed the option
// [keyer.WithTypePrefix] set to true by default, as otherwise it is quite
// common for protobuf messages of different types to share the same
// serialization and therefore cache key.
func Intercept(cache Cache, opts ...Option) grpc.UnaryClientInterceptor {
	opts = append([]Option{WithHashKeyerOpts(keyer.WithTypePrefix(true))}, opts...)
	return InterceptWithMemoizer(New(cache, opts...))
}

// InterceptWithMemoizer is a [grpc.UnaryClientInterceptor] that memoizes gRPC
// clients with the passed [Memoizer].
//
// It is strongly encouraged for the [Memoizer] to take care to produce
// different keys for different input types.
func InterceptWithMemoizer(m Memoizer) grpc.UnaryClientInterceptor {
	// gRPC client interceptors use a different interface from that of servers,
	// taking their reply as an input argument rather than producing it as a
	// return value, so Wrap does not quite work here.
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

	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		callOpts ...grpc.CallOption,
	) error {
		protoReq, ok := req.(proto.Message)
		if !ok {
			errorf("non-proto req %T", req)
			return invoker(ctx, method, req, reply, cc, callOpts...)
		}
		protoReply, ok := reply.(proto.Message)
		if !ok {
			errorf("non-proto reply %T", reply)
			return invoker(ctx, method, req, reply, cc, callOpts...)
		}

		key, err := m.Key(ctx, protoReq)
		if err != nil {
			errorf("Key(%v): %w", req, err)
			return invoker(ctx, method, req, reply, cc, callOpts...)
		}
		if err := getProto(m, key, protoReply); err == nil {
			return nil
		} else if !errors.Is(err, ErrCacheMiss) {
			errorf("getProto(%q): %w", key, err)
		}
		if err := invoker(ctx, method, req, reply, cc, callOpts...); err != nil {
			return err
		}
		if err := addProto(
			m, key, protoReply,
			expiration(ctx, protoReq, protoReply),
			flags(ctx, protoReq, protoReply),
		); err != nil && !errors.Is(err, ErrNotStored) {
			errorf("addProto(%q): %w", key, err)
		}
		return nil
	}
}
