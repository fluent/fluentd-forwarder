package fluentd_forwarder

import (
	"fmt"
)

type Errors []error

func (e Errors) Error() string {
	buf := []byte("Failure due to one or more errors: ")
	for i, e_ := range e {
		if i > 0 {
			buf = append(buf, " / "...)
		}
		buf = append(buf, fmt.Sprintf("%d: %s", i+1, e_.Error())...)
	}
	return string(buf)
}
