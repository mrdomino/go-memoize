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

// Errer receives non-fatal errors that can occur in the memoization logic but
// that do not affect function outputs. It does not receive ErrCacheMiss or
// ErrNotStored, but otherwise if anything goes wrong with cache retreival
// or storage, or with proto serialization or deserialization, it will be
// reported for either recovery or logging.
type Errer interface {
	Error(error)
}

// ErrFunc is an interface wrapper for functions allowing them to be used as
// Errers.
type ErrFunc func(error)

// Error implements Errer.Error for ErrFunc.
func (f ErrFunc) Error(err error) {
	f(err)
}

// ErrorHandler is the default Errer for the memoize package. It is nil by
// default. If it is set to non-nil, and there is not a custom Errer configured
// for a given Memoizer, then it will receive all non-fatal errors. E.g. it may
// make sense to wire this up to a custom log.Errorf function, or even simply
// `log.Printf("ERROR: %v", err)`.
var ErrorHandler Errer
