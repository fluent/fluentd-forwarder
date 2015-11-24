fluentd-forwarder
=================

A lightweight Fluentd forwarder written in Go.

Requirements
------------

- Go v1.4.1 or above
- Set the $GOPATH environment variable to get `fluentd_forwarder`
  under `$GOPATH/bin` directory.

Build Instructions
------------------

To install the required dependencies and build `fluentd_forwarder` do:

```
$ go get github.com/fluent/fluentd-forwarder/entrypoints/build_fluentd_forwarder
$ bin/build_fluentd_forwarder fluentd_forwarder
```

Running `fluentd_forwarder`
---------------------------

```
$ $GOPATH/bin/fluentd_forwarder
```

Without arguments, it simply listens on 127.0.0.1:24224 and forwards the events to 127.0.0.1:24225.

It gracefully stops in response to SIGINT.

If you want to specify where to forward the events, try the following:

```
$ $GOPATH/bin/fluentd_forwarder -to fluent://some-remote-node.local:24224
```

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

* -parallelism

  Number of simultaneous connections used to submit events. It takes effect only when the target is td+http(s).

  ```
  -parallelism 1
  ```

* -log-level

  Logging level. Any one of the following values; CRITICAL, ERROR, WARNING, NOTICE, INFO and DEBUG.

  ```
  -log-level DEBUG
  ```

* -log-file

  Species the path to the log file.  By default logging is performed to the standard error.  It may contain strftime(3)-like format specifications like %Y in any positions.  If the parent directory doesn't exist at the time the logging is performed, all the leading directories are created automatically so you can specify the path like `/var/log/fluentd_forwarder/%Y-%m-%d/fluentd_forwarder.log`

  ```
  -log-file /var/log/fluentd_forwarder.log
  ```

* -config

  Specifies the path to the configuration file.  The syntax is detailed below.

  ```
  -config /etc/fluentd-forwarder/fluentd-forwarder.cfg
  ```


Configuration File
------------------

The syntax of the configuration file is so-called INI format with the name of the primary section being `fluentd-forwarder`.  Each setting is named exactly the same as the command-line counterpart, except for `-config`. (It is not possible to refer to another configuation file from a configuration file)

```
[fluentd-forwarder]
to = fluent://remote.local:24224
buffer-chunk-limit = 16777216
flush-interval = 10s
retry-interval = 1s
```

Dependencies
------------

fluentd_forwarder depends on the following external libraries:

* github.com/ugorji/go/codec
* github.com/op/go-logging
* github.com/jehiah/go-strftime
* github.com/moriyoshi/go-ioextras
* gopkg.in/gcfg.v1

License
-------

The source code and its object form ("Work"), unless otherwise specified, are licensed under the Apache Software License, Version 2.0.  You may not use the Work except in compliance with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.

A portion of the code originally written by Moriyoshi Koizumi and later modified by Treasure Data, Inc. continues to be published and distributed under the same terms and conditions as the MIT license, with its authorship being attributed to the both parties.  It is specified at the top of the applicable source files.
