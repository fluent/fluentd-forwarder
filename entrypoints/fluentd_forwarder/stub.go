// +build !windows

package main

import "os"

func serviceDispatch(name string, handler func([]string, func())) error {
	handler(os.Args, func() {})
	return nil
}
