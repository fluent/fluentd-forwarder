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

It gracefully stops in response to SIGINT.


Command-line Options
--------------------

* -retry-interval

  Retry interval in which connection is tried against the remote agent.

* -conn-timeout

  Connection timeout after which the connection has failed.

* -write-timeout

  Write timeout on wire.

* -flush-interval

  Flush interval in which the events are forwareded to the remote agent .

* -listen-on

  Interface address and port on which the forwarder listens.

* -to

  Host and port to which the events are forwarded.

* -buffer-path

  Directory / path on which buffer files are created. * may be used within the path to indicate the prefix or suffix like var/pre*suf

* -buffer-chunk-limit

  Maximum size of a buffer chunk

Dependencies
------------

fluentd_forwarder depends on the following external libraries:

* github.com/ugorji/go/codec
* github.com/op/go-logging
