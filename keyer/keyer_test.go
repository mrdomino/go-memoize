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
	"crypto"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestHashKeyer(t *testing.T) {
	ctx := t.Context()
	var key string
	var err error

	var keyer Keyer = NewHashKeyer()

	key, err = keyer.Key(ctx, wrapperspb.UInt64(1))
	require.NoError(t, err)
	assert.Equal(t, ":4f8190a08041a67360ceea6c64f9be3ffb59b602", key)

	// different types can hash to the same value.
	key, err = keyer.Key(ctx, wrapperspb.Bool(true))
	require.NoError(t, err)
	assert.Equal(t, ":4f8190a08041a67360ceea6c64f9be3ffb59b602", key)

	// Type prefixes can help with this.
	keyer = NewHashKeyer(WithTypePrefix(true))
	key, err = keyer.Key(ctx, wrapperspb.UInt64(1))
	require.NoError(t, err)
	assert.Equal(t, ":*wrapperspb.UInt64Value:4f8190a08041a67360ceea6c64f9be3ffb59b602", key)
	key, err = keyer.Key(ctx, wrapperspb.Bool(true))
	require.NoError(t, err)
	assert.Equal(t, ":*wrapperspb.BoolValue:4f8190a08041a67360ceea6c64f9be3ffb59b602", key)

	// Global prefixes apply to all keys.
	keyer = NewHashKeyer(WithGlobalPrefix("test"))
	key, err = keyer.Key(ctx, wrapperspb.String(""))
	require.NoError(t, err)
	assert.Equal(t, "test:da39a3ee5e6b4b0d3255bfef95601890afd80709", key)

	// Global prefixes precede type prefixes.
	keyer = NewHashKeyer(WithGlobalPrefix("hi"), WithTypePrefix(true))
	key, err = keyer.Key(ctx, wrapperspb.Float(3.15))
	require.NoError(t, err)
	assert.Equal(t, "hi:*wrapperspb.FloatValue:01d0544a70a81bb9e63f69cadfabe97975a5d659", key)

	// Custom hashes can be used.
	keyer = NewHashKeyer(WithHash(crypto.SHA256))
	key, err = keyer.Key(ctx, wrapperspb.Bytes(nil))
	require.NoError(t, err)
	assert.Equal(t, ":e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", key)
}
