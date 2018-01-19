0.6.0.0 Lars Petersen <info@lars-petersen.net> 2018-01-19

 * Factored out all networking dependencies (sockets, websockets, TLS)
   to package `networking` and only program against abstract interface.

 * New quota option `maxSessions` determines how many simultaneous session
   a single user identity is allowed to have. When reaching the limit,
   the oldest session is terminated. Also added tests for all this.