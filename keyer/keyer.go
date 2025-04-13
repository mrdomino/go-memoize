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

package keyer

import (
	"context"
	"crypto"
	"encoding/hex"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
)

// Keyer produces cache keys for its inputs for use in a memoization cache.
type Keyer interface {
	Key(context.Context, proto.Message) (string, error)
}

// KeyFunc is an interface wrapper to allow using raw functions as Keyers.
type KeyFunc func(context.Context, proto.Message) (string, error)

// Key implements [keyer.Keyer.Key] for [KeyFunc].
func (f KeyFunc) Key(ctx context.Context, m proto.Message) (string, error) {
	return f(ctx, m)
}

// HashKeyer is the default [Keyer] used by go-memoize if one is not supplied.
// It produces keys from a hash of a serialization of its input message.
//
// Note that since [proto serialization is not canonical], this [Keyer] will
// not always produce the same keys for the same inputs, particularly across
// different binaries or different schemas, particularly in the presence of
// unknown fields.
// We try to mitigate this setting [proto.MarshalOptions.Deterministic].
// See the documentation for the limitations of this.
//
// In general, if your application needs stronger canonicalization guarantees
// than protobuf provides, then you should supply a custom Keyer instead of
// using the default one.
//
// [proto serialization is not canonical]: https://protobuf.dev/programming-guides/serialization-not-canonical/
type HashKeyer struct {
	marshal      proto.MarshalOptions
	hashType     crypto.Hash
	globalPrefix string
	typePrefix   bool
}

type HashKeyerOption func(*HashKeyer)

// WithTypePrefix adds a string denoting the concrete type of the message
// to the key as a field. It is recommended to use this if the same cache will
// ever store values of different types, as proto messages can otherwise often
// have the same serialization for different types (e.g. Bool(true) has bit-
// identical serialization to UInt64(1).)
func WithTypePrefix(b bool) HashKeyerOption {
	return func(hk *HashKeyer) {
		hk.typePrefix = b
	}
}

// WithGlobalPrefix adds the passed string as the first element in the key,
// followed by a ':'. It is recommended not to include ':' in the prefix
// itself.
func WithGlobalPrefix(prefix string) HashKeyerOption {
	return func(hk *HashKeyer) {
		hk.globalPrefix = prefix
	}
}

// WithHash allows customizing the type of hash used; the default is
// [crypto.SHA1].
func WithHash(hashType crypto.Hash) HashKeyerOption {
	return func(hk *HashKeyer) {
		hk.hashType = hashType
	}
}

// NewHashKeyer constructs a HashKeyer with the provided options.
func NewHashKeyer(opts ...HashKeyerOption) *HashKeyer {
	hk := &HashKeyer{
		marshal:  proto.MarshalOptions{Deterministic: true},
		hashType: crypto.SHA1,
	}
	for _, opt := range opts {
		opt(hk)
	}
	return hk
}

// Key implements [keyer.Keyer.Key] for [HashKeyer].
func (hk *HashKeyer) Key(_ context.Context, m proto.Message) (string, error) {
	buf, err := hk.marshal.Marshal(m)
	if err != nil {
		return "", err
	}
	hash := hk.hashType.New()
	hash.Write(buf)

	var sb strings.Builder
	sb.WriteString(hk.globalPrefix)
	sb.WriteRune(':')
	if hk.typePrefix {
		sb.WriteString(fmt.Sprintf("%T", m))
		sb.WriteRune(':')
	}
	sb.WriteString(hex.EncodeToString(hash.Sum(nil)))
	return sb.String(), nil
}
