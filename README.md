fluentd-forwarder
=================

Build Instructions
------------------

Set GOPATH environment variable appropriately and do the following to 
get `fluentd_forwarder` under `$GOPATH/bin` directory.

```
$ go get github.com/treasure-data/fluentd-forwarder/entrypoints/fluentd_forwarder
```
(beware of the last component of the URL having an underscore instead of a hyphen)

Currently the program doesn't take any command-line arguments.
Every setting is hard-coded so it listens to 127.0.0.1:24224 for incoming
events, spools those events under the current working directory as files named
like `buffer.*`, and then forwards them to 127.0.0.1:24225.
See entrypoints/main.go for detail.

Note that the default value for the maximum buffer chunk size is 131072,
which is set for testing purpose and may be found too small for the forwarder
to perform well.

It gracefully stops in response to SIGINT.

Dependencies
------------

fluentd_forwarder depends on the following external libraries:

* github.com/ugorji/go/codec
* github.com/op/go-logging
