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

  ```
  -retry-interval 5s
  ```

* -conn-timeout

  Connection timeout after which the connection has failed.

  ```
  -conn-timeout 10s
  ```

* -write-timeout

  Write timeout on wire.

  ```
  -write-timeout 30s
  ```

* -flush-interval

  Flush interval in which the events are forwareded to the remote agent .

  ```
  -flush-interval 5s
  ```

* -listen-on

  Interface address and port on which the forwarder listens.

  ```
  -listen-on 127.0.0.1:24224
  ```

* -to

  Host and port to which the events are forwarded.

  ```
  -to remote-host.local:24225
  -to fluent://remote-host.local:24225
  -to td+https://urlencoded-api-key@/*/*
  -to td+https://urlencoded-api-key@/database/*
  -to td+https://urlencoded-api-key@/database/table
  -to td+https://urlencoded-api-key@endpoint/*/*
  ```

* -ca-certs

  SSL CA certficates to be verified against when the secure connection is used. Must be in PEM format. You can use the [one bundled with td-client-ruby](https://raw.githubusercontent.com/treasure-data/td-client-ruby/master/data/ca-bundle.crt).

  ```
  -ca-certs ca-bundle.crt
  ```


* -buffer-path

  Directory / path on which buffer files are created. * may be used within the path to indicate the prefix or suffix like var/pre*suf

  ```
  -buffer-path /var/lib/fluent-forwarder
  -buffer-path /var/lib/fluent-forwarder/prefix*suffix
  ```

* -buffer-chunk-limit

  Maximum size of a buffer chunk

  ```
  -buffer-chunk-limit 16777216
  ```

* -log-level

  Logging level. Any one of the following values; CRITICAL, ERROR, WARNING, NOTICE, INFO and DEBUG.

  ```
  -log-level DEBUG
  ```

Dependencies
------------

fluentd_forwarder depends on the following external libraries:

* github.com/ugorji/go/codec
* github.com/op/go-logging
