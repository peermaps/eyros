# eyros data format

The eyros data format is split between these sections:

* meta
* staging
* data
* forest of trees (tree0, tree1, tree2, ...)

Each section maps to a file or a file-like storage adaptor provided by
a random-access-storage compatible implementation.

The design of the eyros format is heavily inspired by the bkd paper,
particularly the design of the forest of trees. The primary difference is that
any or all of the dimensions may be intervals instead of a scalar point. This is
accomplished by storing the set of intervals that overlap with each pivot in a
separate data block.

## storage

Many of the sections store points and values.

Points and values can be either fixed or variable in size.

Implementations for fixed or variable serialization and deserialization are
provided by the point and value types with some default implementations for
common types such as floating point numbers, integers, and byte arrays.

All offset pointer types are stored as unsigned 64-bit integers in big endian
format.

## meta

Right now, this file only stores the branch factor.

It will probably be used in the future to store metadata required to implement
atomic operations.

## staging

Save points here before there are enough to fill a tree. The staging area holds a
fixed number of points and values which corresponds to the size of the smallest
tree. 

```
[point0][value0]
[point1][value1]
[point2][value2]
...
```

These points are persisted to disk and parsed representations reside in memory
during the course of the program.

## data

During the batch construction, data points are written when the number of points
in a branch drops below the branch threshold.

Each point is composed of interval and scalar components, but the entire data
block also has an interval that represents the boudning extents of all of its
interval and scalar members. This bounding interval is used during tree merges
to keep data blocks in place and to avoid unnecessary I/O.

```
[length: u32 (bytes)]
[point0][value0]
[point1][value1]
[point2][value2]
...
```

## forest of trees

The forest of trees is implemented as a collection of separate files. The trees
double in size as the tree sequence increases.

A planning algorithm generates sets of trees to merge into sets of output slots
as powers of two times a base record size. The base record size is the same as
the allocated size of the staging store (as a number of records, not necessarily
bytes).

## tree

Each tree does not store the point data itself, merely offsets into the data
file at the leaf nodes.

Each block in the tree (documented below) has an implicit dimension based on the
depth of its position in the tree. The dimension is the depth modulo the
dimension of the point type. The pivot type `T` is the type of the point at
the implicit block dimension.

Each block exists at an byte offset and refers to other blocks at byte offsets
in the same tree file or to byte offsets in the data store if the data bit for
that offset pointer is set.

The branch factor (BF) determines the number of pivots N: `N = pow(2,BF-1)`.

The data bitfield determines whether the corresponding u64 offset is in the
intersecting or buckets array. Each bit in the data bitfield maps to the
concatenation of the intersecting and bucket lists, from lower indexed to higher
indexed bytes, from lower to higher bits.

```
[length: u32 (bytes)]
[pivots: T[N]]
[data bitfield: u8[floor((N+BF+7)/8)]]
[intersecting: u64[N]]
[buckets: u64[BF]]
```

