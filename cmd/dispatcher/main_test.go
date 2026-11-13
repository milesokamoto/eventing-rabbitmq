/*
Copyright 2022 The Knative Authors

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

package main

import "testing"

func TestPrefetchCount(t *testing.T) {
	tests := []struct {
		name     string
		workers  int
		override string
		want     int
		invalid  bool
	}{
		{"default single worker", 1, "", 100, false},
		{"default metadata", 10, "", 100, false},
		{"default high parallelism", 240, "", 240, false},
		{"explicit below former floor", 10, "10", 10, false},
		{"explicit below workers", 100, "10", 10, false},
		{"explicit above workers", 10, "20", 20, false},
		{"zero", 10, "0", 0, true},
		{"negative", 10, "-1", 0, true},
		{"noninteger", 10, "ten", 0, true},
		{"overflow", 10, "65536", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := (envConfig{Parallelism: tt.workers, PrefetchCount: tt.override}).prefetchCount()
			if (err != nil) != tt.invalid || got != tt.want {
				t.Fatalf("got (%d, %v), want (%d, invalid=%v)", got, err, tt.want, tt.invalid)
			}
		})
	}
}
