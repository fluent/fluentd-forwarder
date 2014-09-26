package fluentd_forwarder

import "testing"

func TestNormalizeDatabaseName(t *testing.T) {
	{
		_, err := normalizeDatabaseName("")
		if err == nil { t.FailNow() }
	}
	{
		result, err := normalizeDatabaseName("a")
		if err != nil { t.Logf("%s", err.Error()); t.FailNow() }
		t.Logf("%s", result)
		if result != "a__" { t.Fail() }
	}
	{
		result, err := normalizeDatabaseName("ab")
		if err != nil { t.Logf("%s", err.Error()); t.FailNow() }
		t.Logf("%s", result)
		if result != "ab_" { t.Fail() }
	}
	{
		result, err := normalizeDatabaseName("abc")
		if err != nil { t.Logf("%s", err.Error()); t.FailNow() }
		t.Logf("%s", result)
		if result != "abc" { t.Fail() }
	}
	{
		result, err := normalizeDatabaseName("abcd")
		if err != nil { t.Logf("%s", err.Error()); t.FailNow() }
		t.Logf("%s", result)
		if result != "abcd" { t.Fail() }
	}
	{
		result, err := normalizeDatabaseName("abc.def.ghi")
		if err != nil { t.Logf("%s", err.Error()); t.FailNow() }
		t.Logf("%s", result)
		if result != "abc_def_ghi" { t.Fail() }
	}
}
