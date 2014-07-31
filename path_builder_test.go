package fluentd_forwarder

import (
	"testing"
	"time"
)

func Test_BuildJournalPath(t *testing.T) {
	info := BuildJournalPath(
		"test",
		JournalFileType('b'),
		time.Date(2014, 1, 1, 0, 0, 0, 0, time.UTC),
		0x0,
	)
	t.Logf("%+v", info)
	if len(info.UniqueId) != len(info.TSuffix) { t.Fail() }
	if info.VariablePortion != "test.b4eedd5baba000000" { t.Fail() }
}

