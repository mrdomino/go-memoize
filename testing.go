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
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

// LocalCache is a test-only in-memory [Cache]. Stores always succeed unless
// Full is set to true. Gets always succeed if there is an unexpired [Item] at
// that key unless Down is set to true.
//
// This type should be constructed via [NewLocalCache] as it contains an
// unexported map field.
type LocalCache struct {
	Full, Down atomic.Bool

	advancedTime atomic.Int64

	lock sync.RWMutex
	data map[string]*Item
}

var _ Cache = (*LocalCache)(nil)

func NewLocalCache() *LocalCache {
	return &LocalCache{
		data: make(map[string]*Item),
	}
}

func (c *LocalCache) Add(item *Item) error {
	if c.Full.Load() {
		return errors.ErrUnsupported
	}
	if item.Expiration != 0 && item.Expiration <= int32((24*time.Hour*31).Seconds()) {
		item.Expiration = int32(c.Now().Add(time.Duration(item.Expiration) * time.Second).Unix())
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	if old, ok := c.data[item.Key]; ok && (old.Expiration == 0 || !time.Unix(int64(old.Expiration), 0).Before(c.Now())) {
		return memcache.ErrNotStored
	}
	c.data[item.Key] = item
	return nil
}

func (c *LocalCache) Get(key string) (*Item, error) {
	if c.Down.Load() {
		return nil, errors.ErrUnsupported
	}
	c.lock.RLock()
	item, ok := c.data[key]
	c.lock.RUnlock()
	if !ok {
		return nil, ErrCacheMiss
	}
	now := c.Now()
	if item.Expiration != 0 && time.Unix(int64(item.Expiration), 0).Before(now) {
		c.lock.Lock()
		defer c.lock.Unlock()
		item, ok = c.data[key]
		if !ok {
			return nil, ErrCacheMiss
		}
		if item.Expiration != 0 && time.Unix(int64(item.Expiration), 0).Before(now) {
			delete(c.data, key)
			return nil, ErrCacheMiss
		}
	}
	return item, nil
}

// Now implements [time.Now] with the skew applied by [LocalCache.AdvanceTime].
func (c *LocalCache) Now() time.Time {
	return time.Now().Add(time.Duration(c.advancedTime.Load()))
}

// AdvanceTime advances this cache’s clock by the passed duration.
func (c *LocalCache) AdvanceTime(d time.Duration) {
	c.advancedTime.Add(int64(d))
}

// NilCache is a [Cache] that never stores or retrieves anything.
//
// Gets fail with [ErrCacheMiss], and Adds fail with [ErrNotStored]. As both of
// these errors are ignored by the error handling logic in this library, this
// means a function wrapped with NilCache will report no errors.
//
// This is sort of lying, in that [ErrNotStored] from [memcache.Client.Add] is
// supposed to only be returned if there already was a value for that key in
// the cache. But it is not hard to imagine a cache that winds up behaving like
// this in practice on some pathological workload, say getting an Add for the
// same key from elsewhere before every Add, but then having that key evicted
// before every Get.
var NilCache = (*nilCache)(nil)

type nilCache struct{}

func (*nilCache) Add(*Item) error {
	return ErrNotStored
}

func (*nilCache) Get(string) (*Item, error) {
	return nil, ErrCacheMiss
}
