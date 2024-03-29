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
    wasmSource: fetch('2d.wasm')
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

## `var db = await eyros{N}d(opts)`

Open a database for the given dimension from:

* `opts.wasmSource` - wasm data contained in an arraybuffer, typed array, [Response][],
  or a promise resolving to any of those types
* `opts.wasmStream` - wasm source to pass directly to `WebAssembly.instantiateStreaming()`
  without inferring whether to stream or not
* `opts.wasmModule` - already-created WebAssembly.Module instance
* `opts.storage(name)` - function that returns a random-access interface
* `opts.getId(value)` - return a Uint8Array `id` for a given `value`.
  defaults to returning the value
* `opts.branchFactor` - number of non-intersecting branches per node. default: `6`
* `opts.maxDepth` - maximum tree depth before splitting into a separate tree file. default: `8`
* `opts.maxRecords` - maximum number of records to store per tree file. default: `20_000`
* `opts.inline` - threshold under which records will be written out in a list rather than branches.
  default: `500`
* `opts.treeCacheSize` - maximum number of trees to cache in the lru. default: `1000`
* `opts.rebuildDepth` - number of levels to rebuild each batch in an optimization pass: default `2`
* `opts.debug` - optionally supply a function to receive internal debug messages

One of `opts.wasmSource` or `opts.wasmModule` must be provided.

The `opts.storage` function must return random-access instances that implement a
`.length` property, `.len(cb)`, or `.length(cb)` function which returns the size
in bytes that has been allocated for the given file.

Files to supply to `opts.wasmSource` can be obtained from the root of this
package under the convention `${N}d.wasm` for a dimension `N`.

[Response]: https://developer.mozilla.org/en-US/docs/Web/API/Response

## `await db.batch(rows, opts={})`

Insert `rows`, an array of operations to perform on the database.

Each `row` must have a `row.type` set to `'insert'` or `'delete'` with a `row.point`
and a `row.value` Uint8Array (for inserts) or a `row.id` (for deletes).

`row.point` is an n-dimensional array of scalar floats or 2-item arrays of `[min,max]` floats for
each dimension.

Optionally provide:

* `opts.rebuildDepth` - depth to calculate an optimizing rebuild up to (default: 2)
* `opts.errorIfMissing` - whether to raise an error if deleted records are not found before removal

## `await db.sync()`

Write database changes to the underlying data storage.

## `var q = await db.query(bbox, opts={})`

Return an async iterator `q` containing all records from the database that intersect the `bbox`.

Obtain results by calling `row = await q.next()` until it yields a falsy result.
Each `row` is a 2-item array of the form `[point,value]` and each `point` is an n-dimensional array
of scalar floats or 2-item arrays of `[min,max]` floats for each dimension.

`bbox` is an array of the form `[minX,minY,...,maxX,maxY,...]`.
For 2 dimensions, the `bbox` would be `[west,south,east,north]` for `lon,lat` coordinates.

Optionally provide a function `opts.trace(tr)` which will receive a `tr` object just before the
corresponding file is read from storage:

* `tr.id` - integer id for this tree file
* `tr.file` - file path string for the tree
* `tr.bbox` - bounding extents of the tree in `minX,minY,...,maxX,maxY,...]` form

The motivating use case for `opts.trace` is to cancel open requests for content which is no longer
in view when panning a map. The information from the trace can be passed to the storage layer to
make these decisions.

# install

```
npm install eyros
```

# license

[license zero parity 7.0.0](https://paritylicense.com/versions/7.0.0.html)
and [apache 2.0](https://www.apache.org/licenses/LICENSE-2.0.txt)
(contributions)
