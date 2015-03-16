//
// Fluentd Forwarder
//
// Copyright (C) 2014 Treasure Data, Inc.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//    http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import "testing"

func TestNormalizeDatabaseName(t *testing.T) {
	{
		_, err := normalizeDatabaseName("")
		if err == nil {
			t.FailNow()
		}
	}
	{
		result, err := normalizeDatabaseName("a")
		if err != nil {
			t.Logf("%s", err.Error())
			t.FailNow()
		}
		t.Logf("%s", result)
		if result != "a__" {
			t.Fail()
		}
	}
	{
		result, err := normalizeDatabaseName("ab")
		if err != nil {
			t.Logf("%s", err.Error())
			t.FailNow()
		}
		t.Logf("%s", result)
		if result != "ab_" {
			t.Fail()
		}
	}
	{
		result, err := normalizeDatabaseName("abc")
		if err != nil {
			t.Logf("%s", err.Error())
			t.FailNow()
		}
		t.Logf("%s", result)
		if result != "abc" {
			t.Fail()
		}
	}
	{
		result, err := normalizeDatabaseName("abcd")
		if err != nil {
			t.Logf("%s", err.Error())
			t.FailNow()
		}
		t.Logf("%s", result)
		if result != "abcd" {
			t.Fail()
		}
	}
	{
		result, err := normalizeDatabaseName("abc.def.ghi")
		if err != nil {
			t.Logf("%s", err.Error())
			t.FailNow()
		}
		t.Logf("%s", result)
		if result != "abc_def_ghi" {
			t.Fail()
		}
	}
}
