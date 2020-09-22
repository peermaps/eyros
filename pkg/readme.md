# eyros

multi-dimensional interval database

This database is written in rust and compiled to wasm with a javascript
interface.

The data structures are based on bkd and interval trees and designed for:

* high batch write performance
* peer to peer distribution and query-driven sparse replication
* geospatial and time-series use cases

# example

In this example, we populate an in-memory database with 2-dimensional points and
intervals. Any points or intervals that intersect the bounding box from `-10` to
`+10` on the x and y coordinates is returned in the query result.

``` js
const RAM = require('random-access-memory')
const eyros = require('eyros/2d')

;(async function () {
  var db = await eyros({
    storage: RAM,
    wasmSource: await (await fetch('2d.wasm')).arrayBuffer()
  })
  await db.batch([
    { type:'insert', point:[+1,+2], value: Uint8Array.from([97,98,99]) },
    { type:'insert', point:[-5,+4], value: Uint8Array.from([100,101]) },
    { type:'insert', point:[+9,-3], value: Uint8Array.from([102,103,104]) },
    { type:'insert', point:[+5,-15], value: Uint8Array.from([105,106]) },
    { type:'insert', point:[[+5,+12],[-15,-3]], value: Uint8Array.from([105,106]) },
    { type:'insert', point:[[-20,-11],[-8,+2]], value: Uint8Array.from([107]) },
  ])
  var q = await db.query([-10,-10,+10,+10])
  var row
  while (row = await q.next()) {
    console.log('row=',row)
  }
})()
```

To use the database from node, you can use `fs.readFileSync()` with
`require.resolve()` to get the wasmSource:

``` js
const RAM = require('random-access-memory')
const eyros = require('eyros/2d')
const fs = require('fs')

;(async function () {
  var db = await eyros({
    storage: RAM,
    wasmSource: fs.readFileSync(require.resolve('eyros/2d.wasm'))
  })
  await db.batch([
    { type:'insert', point:[+1,+2], value: Uint8Array.from([97,98,99]) },
    { type:'insert', point:[-5,+4], value: Uint8Array.from([100,101]) },
    { type:'insert', point:[+9,-3], value: Uint8Array.from([102,103,104]) },
    { type:'insert', point:[+5,-15], value: Uint8Array.from([105,106]) },
    { type:'insert', point:[[+5,+12],[-15,-3]], value: Uint8Array.from([105,106]) },
    { type:'insert', point:[[-20,-11],[-8,+2]], value: Uint8Array.from([107]) },
  ])
  var q = await db.query([-10,-10,+10,+10])
  var row
  while (row = await q.next()) {
    console.log('row=',row)
  }
})()
```

# api

```
const eyros2d = require('eyros/2d')
const eyros3d = require('eyros/3d')
const eyros4d = require('eyros/4d')
const eyros5d = require('eyros/5d')
const eyros6d = require('eyros/6d')
const eyros7d = require('eyros/7d')
const eyros8d = require('eyros/8d')
```

The precision for all databases is presently f32.

## `var db = await eyros${N}d(opts)`

Open a database for the given dimension from:

* `opts.wasmSource` - arraybuffer or typed array of wasm data
* `opts.wasmModule` - already-created WebAssembly.Module instance
* `opts.storage(name)` - function that returns a random-access interface

One of `opts.wasmSource` or `opts.wasmModule` must be provided.

The `opts.storage` function must return random-access instances that implement a
`.length` property, `.len(cb)`, or `.length(cb)` function which returns the size
in bytes that has been allocated for the given file.

Files to supply to `opts.wasmSource` can be obtained from the root of this
package under the convention `${N}d.wasm` for a dimension `N`.

# install

```
npm install eyros
```

# license

[license zero parity 7.0.0](https://paritylicense.com/versions/7.0.0.html)
and [apache 2.0](https://www.apache.org/licenses/LICENSE-2.0.txt)
(contributions)