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
format. An offset pointer of `0` means that there is no further data. If the
offset pointer is greater than 0, it should be decremented by `1` to get the
file offset.

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
in a branch drops below the max data threshold.

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
dimension of the point type, just like with k-d trees. The pivot type `T` is the
type of the point at the implicit block dimension.

Each block exists at an byte offset and refers to other blocks at byte offsets
in the same tree file or to byte offsets in the data store if the data bit for
that offset pointer is set.

The branch factor (BF) determines the number of pivots N: `N=2*BF-3`.

The data bitfield determines whether the corresponding u64 offset in the
intersecting or buckets array points to a location in the data store (`true`),
or to an offset into current tree (`false`) or if the pointer is empty
(`false`). Each bit in the data bitfield maps to the concatenation of the
intersecting and bucket lists, from lower indexed to higher indexed bytes, from
lower to higher bits.

```
[length: u32 (bytes)]
[pivots: T[N]]
[data bitfield: u8[floor((N+BF+7)/8)]]
[intersecting: u64[N]]
[buckets: u64[BF]]
```

Note that the u64 offsets are set to `0` to indicate there is no further data.
If an offset is greater than `0`, the value read from the structure should be
subtracted by `1` to get the correct file offset.

The length of the block in bytes refers to the whole block, which includes the
4-byte u32 length property itself.

The purpose of the fields in these blocks is to batch together several layers of
the interval tree structure in order to reduce the number of storage reads. A
similar idea is used by B-trees where blocks contain a list of pivots bounded on
each side by a bucket. The B-tree technique must be adapted somewhat here
because an interval could be intersected by more than one pivot, which would
render the tree structure unsuitable for partitioning the space at each level.

To achieve the performance gains of the B-tree technique on an interval tree,
we can calculate a sweep of `N` pivot points where `N=BF*2-3` which attempt to
balance the bucket allocation. These pivot points are sorted in ascending order
based on the point data for the dimension under consideration for the given
level of the tree. This collection of pivots comprise a binary interval tree
with pointers to sets of intersecting intervals at each pivot, but with buckets
connected only to the final level of the tree.

Here is an example for `BF=5` with pivots (P) intersecting pointers (I) and
bucket pointers (B):

```
             P3
           _/ | \_
         _/   |   \_
        /    I3     \
       P1            P5
     /  | \        /  | \
   P0  I1  P2    P4  I5 P6
 / | \   / | \  / | \  / | \
B0 I0  B1 I2  B2 I4  B3 I6 B4

```

The serialization for this example tree from top to bottom, left to right,
assuming that I1,I3,I4,B1 and B3 fall below the max data threshold and point to
data blocks:

```
[block length in bytes as u32]
[P0] [P1] [P2] [P3] [P4] [P5] [P6]
[0b00011010 = 0x1a (byte)]
[0b00000101 = 0x05 (byte)]
[I0] [I1] [I2] [I3] [I4] [I5] [I6]
[B0] [B1] [B2] [B4]
```

The block length is included to keep open the option to have variable-sized
pivot values or pointers. Without those considerations, the length field could
be omitted.

