A pure Haskell MQTT client and server implementation
====================================================

## Project goal

This project aims to supply a rock-solid MQTT implementation suitable for
production use.

The planned features are:

  - A client implementation with integrated broker which allows one client to be
    used by several threads/consumers simultaneously.
  - A server implementation capable of handing and serving several thousands of
    connections.
  - TLS and WebSocket connections.
  - An interface for pluggable authentication and authorization.
  - High test and benchmark coverage.

## Project state

This is a work in progress. A first useful release is planned for the end of 2016.

The following has already been implemented:

  - Binary protocol handling (parsing and serializing).
  - Client connection handling.
  - High-performance and concurrent message routing (see `RoutingTree`).

## Documentation

[Haddock source code documentation is available here.](http://mqtt.lpeterse.de)

## License

Permission is hereby granted under the terms of the MIT license:

> Copyright (c) 2016 Lars Petersen
>
> Permission is hereby granted, free of charge, to any person obtaining
> a copy of this software and associated documentation files (the
> "Software"), to deal in the Software without restriction, including
> without limitation the rights to use, copy, modify, merge, publish,
> distribute, sublicense, and/or sell copies of the Software, and to
> permit persons to whom the Software is furnished to do so, subject to
> the following conditions:
>
> The above copyright notice and this permission notice shall be included
> in all copies or substantial portions of the Software.
>
> THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
> EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
> MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
> IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
> CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
> TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
> SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
