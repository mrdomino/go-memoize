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
	"github.com/bradfitz/gomemcache/memcache"
	"google.golang.org/protobuf/proto"
)

type Item = memcache.Item

var (
	ErrCacheMiss = memcache.ErrCacheMiss
	ErrNotStored = memcache.ErrNotStored
)

// Cache implements a minimal subset of [memcache.Client] for use elsewhere in
// this library.
type Cache interface {
	Add(*Item) error
	Get(string) (*Item, error)
}

// addProto combines proto serialization with caching, allowing callers to
// early-return in one place from success or failure of storing a proto message
// in a cache.
func addProto(cache Cache, key string, m proto.Message, expiration int32, flags uint32) error {
	buf, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return cache.Add(&Item{
		Key:        key,
		Value:      buf,
		Flags:      flags,
		Expiration: expiration,
	})
}

// getProto combines cache retrieval with proto deserialization, allowing
// callers to early-return from success or failure of a complete load/unmarshal
// operation.
func getProto(cache Cache, key string, m proto.Message) error {
	item, err := cache.Get(key)
	if err != nil {
		return err
	}
	return proto.Unmarshal(item.Value, m)
}
