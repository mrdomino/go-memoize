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
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

// LocalCache is a test-only in-memory cache. Stores always succeed unless its
// Full is set to true. Gets always succeed if there is an unexpired Item at
// that key unless Down is set to true.
type LocalCache struct {
	lock sync.RWMutex
	data map[string]*Item

	Full, Down bool

	AdvancedTime time.Duration
}

var _ Cache = (*LocalCache)(nil)

func NewLocalCache() *LocalCache {
	return &LocalCache{
		data: make(map[string]*Item),
	}
}

func (c *LocalCache) Add(item *Item) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.Full {
		return errors.ErrUnsupported
	}
	if item.Expiration != 0 && item.Expiration <= int32((24*time.Hour*31).Seconds()) {
		item.Expiration = int32(c.Now().Add(time.Duration(item.Expiration) * time.Second).Unix())
	}
	if old, ok := c.data[item.Key]; ok && (old.Expiration == 0 || !time.Unix(int64(old.Expiration), 0).Before(c.Now())) {
		return memcache.ErrNotStored
	}
	c.data[item.Key] = item
	return nil
}

func (c *LocalCache) Get(key string) (*Item, error) {
	c.lock.RLock()
	if c.Down {
		c.lock.RUnlock()
		return nil, errors.ErrUnsupported
	}
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
		if item.Expiration != 0 && time.Unix(int64(item.Expiration), 0).Before(now) {
			delete(c.data, key)
			return nil, ErrCacheMiss
		}
	}
	return item, nil
}

func (c *LocalCache) Now() time.Time {
	return time.Now().Add(c.AdvancedTime)
}

func (c *LocalCache) AdvanceTime(d time.Duration) {
	c.AdvancedTime += d
}

// NilCache is a cache that never stores or retrieves anything.
type NilCache struct{}

func (*NilCache) Add(*Item) error {
	return errors.ErrUnsupported
}

func (*NilCache) Get(string) (*Item, error) {
	return nil, errors.ErrUnsupported
}
