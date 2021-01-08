# directory structure

* `staging/[0-9a-f]{2}`
* `staging/clusters`
* `mtree/00` (root tree)
* `mtree/[0-9a-f]{2}` (increasing powers of 2-sized trees)
* `tree(/[0-9a-f]{2}){8}`

# staging/clusters

todo

# `staging/[0-9a-f]{2}`

todo

# tree

Each tree is its own file with a file path based on the tree's u64 id. Internally, the tree file
will contain branches which themselves contain nodes.

Trees are prefixed by the "tree/" directory path and the remainder of the tree's path is derived
from its u64 id by converting to pairs of hexadecimal digits from most-to-least significant
digit-pairs with the first 7 pairs as subdirectories and the last pair as the file name. Each pair
is zero-padded with the first hexadecimal digit as the most-significant digit and the second the
least significant. Here is a table of tree id examples for `tree_id` values given in decimal:

```
tree_id=1              path=tree/00/00/00/00/00/00/00/01
tree_id=555            path=tree/00/00/00/00/00/00/02/2b
tree_id=1099505501429  path=tree/00/00/00/ff/ff/a2/84/f5
```

Each tree contains these fields:

* `record_count` (`varint`) - number of records in this tree (excludes external trees)
* `bounds` (`X,Y,...,X,Y,...`) - bounding extents of all records. tuple of minimums for each
  dimension followed by list of maximums `(minX,minY,...,maxX,maxY,...)`
// * `start_dim` (`varint`) - dimension that the tree starts at
* `root` (`node`) - root of the tree

# branch

* `pivot_len` (`varint`) - length of pivots list to follow
* `pivots` `(X,Y,...)%depth` - list of pivots. type is modulo tree depth
* `intersections` (`[node]`) - list of nodes (length=`pivot_len`) that intersect the pivots list
* `nodes` (`[node]`) - list of nodes (length=`pivot_len+1`) split by, but not intersecting, the
  pivots

# node

* `n` (`u32`) - branch offset, external tree id, or data length (see calculation)

depending on the value of `n % 2`, the node is a:

* `0` - branch pointer. `branch_offset = n>>1`.
  byte offset to a branch in the current tree file.
  this offset is relative to the beginning of the tree file.
* `1` - data block. `(data_len, ref_len) = ((n>>1)&0xffff, n>>17)`.
  the number of inline records and inline refs to read after the `n` u32.
  a bitfield of size `floor((n/2+7)/8)` comes before the inline records where a 1 indicates that the
  record has been deleted.
  `data_len` inline records are followed by `ref_len` inline refs.
  each inline ref is a varint.

A value of `n=1` indicates a node for an empty set (data block where `data_len=0`).

The type of `n` as a `u32` naturally limits the size of a tree to slightly more than 1 gigabyte,
with a maximum offset size of 1.43 GB (1.33 GiB). In practice, these files should be at most far
smaller for the purpose of various rebalancing operations. Trees can link to each other after all.

A `u32` for `n` is slightly wasteful compared to a varint but greatly simplifies tree serialization,
as you can calculate the size of a branch without foreknowledge of the offset and therefore size of
linked-to branches.
