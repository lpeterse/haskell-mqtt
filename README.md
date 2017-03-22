A pure Haskell MQTT client and server implementation
====================================================

## Project goal

This project aims to supply a rock-solid MQTT 3.1.1 implementation suitable for production use.

The planned features are:

  - A broker implementation capable of handing and serving several thousands of
    connections.
  - A client implementation with integrated broker which allows one client to be
    used by several threads/consumers simultaneously.
  - TLS and WebSocket connections.
  - An interface for pluggable authentication and authorization.
  - High test and benchmark coverage.

## Project state (2017-03-22)

  - The broker implementation is nearly feature complete and well-tested.
    The [hummingbird](https://github.com/lpeterse/haskell-hummingbird) project
    is a full-fledged broker built on-top of this library.
  - The client implementation went out of focus for now and is currently
    commented out. It's still a planned feature and is essentially a low
    hanging fruit as all the protocol parsers etc. are already in place.

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
