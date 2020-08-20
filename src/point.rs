use std::cmp::Ordering;
use std::ops::{Div,Add};
use failure::{Error,format_err};
use std::fmt::Debug;
use std::mem::size_of;
use crate::order;
use desert::{ToBytes,FromBytes,CountBytes};

/// `(branch_offset, tree depth)` tuple for branch traversal
pub type Cursor = (u64,usize);

/// File offset into bulk data
pub type Block = u64;

/// Points (scalar or interval) must implement these methods.
/// There's a lot going on here, so you'll most likely want to use one of the
/// built-in implementations rather than write your own.
///
/// Below, the term "element" refs to a value contained in a point which could
/// be a scalar or interval.
/// For example, for the point `((-2.0,4.5), 6.0, (9.0,11.0))`,
/// each of `(-2.0,4.5)`, `6.0`, and `(9.0,11.0)` is an "element".
///
/// Presently only types with static sizes are supported.

pub trait Point: Copy+Clone+Send+Sync+Debug+ToBytes+FromBytes+CountBytes {
  /// Bounding-box corresponding to `(min,max)` as used by `db.query(bbox)`.
  type Bounds: Copy+Clone+Send+Sync+Debug+ToBytes+FromBytes+CountBytes;

  /// Range corresponding to `((minX,maxX),(minY,maxY),...)`
  type Range: Point+Copy+Clone+Send+Sync+Debug+ToBytes+FromBytes+CountBytes;

  /// Compare elements at a level of tree depth. The dimension under
  /// consideration alternates each level, so you'll likely want the element
  /// at an index corresponding to `level % dimension`.
  fn cmp_at (&self, other: &Self, level: usize) -> Ordering where Self: Sized;

  /// For intervals, calculate the midpoint of the greater (upper) interval
  /// bound (ex: `iv.1`) for two intervals, returning a new interval where both
  /// elements are the midpoint result.
  /// For scalars, return the midpoint of two scalars as a scalar.
  fn midpoint_upper (&self, other: &Self) -> Self where Self: Sized;

  /// Return the byte presentation for the element corresponding to the tree
  /// depth `level` for the purpose of making a pivot. If you have an interval
  /// type, return the upper bound.
  fn serialize_at (&self, level: usize, dst: &mut [u8]) -> Result<usize,Error>;

  /// Get the number of dimensions for this point type.
  fn dim () -> usize;

  /// Return whether the current point intersects with a bounding box.
  fn overlaps (&self, bbox: &Self::Bounds) -> bool;

  /// Return the size in bytes of the pivot-form of the element corresponding to
  /// the tree depth `level`.
  fn pivot_bytes_at (&self, level: usize) -> usize;

  /// Calculate the number of bytes to read from `buf` for the tree depth
  /// `level`.
  fn count_bytes_at (buf: &[u8], level: usize) -> Result<usize,Error>;

  /// Return a set of `(branch_offset,tree_depth)` tuples (`Cursors`) for
  /// sub-branches to load next and a set of `u64` (`Blocks`) to read data from
  /// according to a traversal of the branch data in `buf` at the tree depth
  /// `level` and subject to the bounds given in `bbox`.
  fn query_branch (buf: &[u8], bbox: &Self::Bounds, branch_factor: usize,
    level: usize) -> Result<(Vec<Cursor>,Vec<Block>),Error>;

  /// Return a bounding box for a set of coordinates, if possible.
  fn bounds (coords: &Vec<Self>) -> Option<Self::Bounds>;

  /// Return a Range corresponding to a bounding box.
  /// This involves transposing the items. For example:
  ///
  /// `((-1.0,0.0,-4.0),(3.0,0.8,2.5)` (bbox)
  ///
  /// becomes
  ///
  /// `((-1.0,3.0),(0.0,0.8),(-4.0,2.5))` (range)
  fn bounds_to_range (bbox: Self::Bounds) -> Self::Range;

  /// Return a string representation of the element in a buffer slice
  /// corresponding to the tree depth level.
  fn format_at (buf: &[u8], level: usize)
    -> Result<String,Error>;
}

pub trait Num<T>: PartialOrd+Copy+Send+Sync+ToBytes+FromBytes+CountBytes
  +Debug+Scalar+From<u8>+Div<T,Output=T>+Add<T,Output=T> {}
impl<T> Num<T> for T where T: PartialOrd+Copy+Send+Sync
  +ToBytes+FromBytes+CountBytes
  +Debug+Scalar+From<u8>+Div<T,Output=T>+Add<T,Output=T> {}

/// Types representing a single value (as opposed to an interval, which has
/// minimum and maximum values).
///
/// This trait has no required methods.
pub trait Scalar: Copy+Sized {}
impl Scalar for f32 {}
impl Scalar for f64 {}
impl Scalar for u8 {}
impl Scalar for u16 {}
impl Scalar for u32 {}
impl Scalar for u64 {}
impl Scalar for i8 {}
impl Scalar for i16 {}
impl Scalar for i32 {}
impl Scalar for i64 {}

trait Coord<T> {
  fn cmp (&self, other: &Self) -> Option<Ordering>;
  fn midpoint_upper (&self, other: &Self) -> Self;
  fn upper (&self) -> T;
  fn overlaps (&self, a: &T, b: &T) -> bool;
  fn bounds (coords: Vec<&Self>) -> Option<(T,T)>;
}

impl<T> Coord<T> for T where T: Scalar+PartialOrd+Num<T> {
  fn cmp (&self, other: &T) -> Option<Ordering> {
    self.partial_cmp(&other)
  }
  fn midpoint_upper (&self, other: &Self) -> Self {
    (*self + *other) / 2.into()
  }
  fn upper (&self) -> T { *self }
  fn overlaps (&self, min: &T, max: &T) -> bool {
    *min <= *self && *self <= *max
  }
  fn bounds (coords: Vec<&Self>) -> Option<(T,T)> {
    if coords.len() == 0 { return None }
    let mut min = coords[0];
    let mut max = coords[0];
    for i in 1..coords.len() {
      let c = coords[i];
      match c.cmp(min) {
        None => { return None },
        Some(Ordering::Less) => { min = c },
        _ => {}
      };
      match c.cmp(max) {
        None => { return None },
        Some(Ordering::Greater) => { max = c },
        _ => {}
      };
    }
    Some((*min,*max))
  }
}

impl<T> Coord<T> for (T,T) where T: Scalar+PartialOrd+Num<T> {
  fn cmp (&self, other: &Self) -> Option<Ordering> {
    if self.0 <= other.1 && other.0 <= self.1 {
      Some(Ordering::Equal)
    } else {
      self.0.partial_cmp(&other.0)
    }
  }
  fn midpoint_upper (&self, other: &Self) -> Self {
    let x = self.1/2.into() + other.1/2.into();
    (x,x)
  }
  fn upper (&self) -> T { self.1 }
  fn overlaps (&self, min: &T, max: &T) -> bool {
    *min <= self.1 && self.0 <= *max
  }
  fn bounds (coords: Vec<&Self>) -> Option<(T,T)> {
    if coords.len() == 0 { return None }
    let mut min = coords[0].0;
    let mut max = coords[0].1;
    for i in 1..coords.len() {
      let c = coords[i];
      match (c.0).cmp(&min) {
        None => { return None },
        Some(Ordering::Less) => { min = c.0 },
        _ => {}
      };
      match (c.1).cmp(&max) {
        None => { return None },
        Some(Ordering::Greater) => { max = c.1 },
        _ => {}
      };
    }
    Some((min,max))
  }
}

macro_rules! impl_point {
  (($($T:tt),+),($($U:tt),+),($($i:tt),+),$dim:expr) => {
    impl<$($T),+> Point for ($($U),+)
    where $($T: Num<$T>),+ {
      type Bounds = (($($T,)+),($($T,)+));
      type Range = ($(($T,$T),)+);
      fn cmp_at (&self, other: &Self, level: usize) -> Ordering {
        let order = match level%Self::dim() {
          $($i => Coord::cmp(&self.$i, &other.$i),)+
          _ => panic!("match case beyond dimension")
        };
        match order { Some(x) => x, None => Ordering::Less }
      }
      fn midpoint_upper (&self, other: &Self) -> Self {
        ($(
          Coord::midpoint_upper(&self.$i, &other.$i)
        ),+)
      }
      fn serialize_at (&self, level: usize, dst: &mut [u8])
      -> Result<usize,Error> {
        match level%Self::dim() {
          $($i => self.$i.upper().write_bytes(dst),)+
          _ => panic!("match case beyond dimension")
        }
      }
      fn dim () -> usize { $dim }
      fn overlaps (&self, bbox: &Self::Bounds) -> bool {
        $(Coord::overlaps(&self.$i, &(bbox.0).$i, &(bbox.1).$i) &&)+ true
      }
      fn pivot_bytes_at (&self, i: usize) -> usize {
        match i % $dim {
          $($i => size_of::<$T>(),)+
          _ => panic!("dimension out of bounds")
        }
      }
      fn count_bytes_at (buf: &[u8], i: usize) -> Result<usize,Error> {
        match i % $dim {
          $($i => $T::count_from_bytes(buf),)+
          _ => panic!("dimension out of bounds")
        }
      }
      fn query_branch (buf: &[u8], bbox: &Self::Bounds, bf: usize, level: usize)
      -> Result<(Vec<Cursor>,Vec<Block>),Error> {
        let mut cursors = vec![];
        let mut blocks = vec![];

        let n = order::order_len(bf);
        let mut offset = 0;
        let mut pivots = ($({ $i; vec![] }),+);
        for _i in 0..n {
          match level % $dim {
            $($i => {
              let (size,x) = $T::from_bytes(&buf[offset..])?;
              (pivots.$i).push(x);
              offset += size;
            },)+
            _ => panic!["dimension out of bounds"]
          };
        }
        let d_start = offset; // data bitfield
        let i_start = d_start + (n+bf+7)/8; // intersections
        let b_start = i_start + n*size_of::<u64>(); // buckets
        let b_end = b_start+bf*size_of::<u64>();
        ensure_eq!(b_end, buf.len(), "unexpected block length");

        let mut bcursors = vec![0];
        let mut bitfield: Vec<bool> = vec![false;bf]; // which buckets
        while !bcursors.is_empty() {
          let c = bcursors.pop().unwrap();
          let i = order::order(bf, c);
          let cmp = match level % $dim {
            $($i => {
              let pivot = (pivots.$i)[i];
              (
                (bbox.0).$i <= pivot,
                pivot <= (bbox.1).$i
              )
            },)+
            _ => panic!["dimension out of bounds"]
          };
          let is_data = ((buf[d_start+i/8]>>(i%8))&1) == 1;
          let i_offset = i_start + i*8;
          // intersection:
          let offset = u64::from_be_bytes([
            buf[i_offset+0], buf[i_offset+1],
            buf[i_offset+2], buf[i_offset+3],
            buf[i_offset+4], buf[i_offset+5],
            buf[i_offset+6], buf[i_offset+7],
          ]);
          if is_data && offset > 0 {
            blocks.push(offset-1);
          } else if offset > 0 {
            cursors.push((offset-1,level+1));
          }
          // internal branches:
          if cmp.0 && c*2+1 < n { // left internal
            bcursors.push(c*2+1);
          } else if cmp.0 { // left branch
            bitfield[i/2] = true;
          }
          if cmp.1 && c*2+2 < n { // right internal
            bcursors.push(c*2+2);
          } else if cmp.1 { // right branch
            bitfield[i/2+1] = true;
          }
          // internal leaves are even integers in (0..n)
          // which map to buckets `i/2+0` and/or `i/2+1`
          // depending on left/right comparisons
          /*                7
                     3             11
                  1     5       9      13
                0   2 4  6    8  10  12  14
            B: 0  1  2  3   4  5   6   7   8
          */
        }
        for (i,b) in bitfield.iter().enumerate() {
          if !b { continue }
          let j = i+n;
          let is_data = (buf[d_start+j/8]>>(j%8))&1 == 1;
          let offset = u64::from_be_bytes([
            buf[b_start+i*8+0], buf[b_start+i*8+1],
            buf[b_start+i*8+2], buf[b_start+i*8+3],
            buf[b_start+i*8+4], buf[b_start+i*8+5],
            buf[b_start+i*8+6], buf[b_start+i*8+7]
          ]);
          if offset > 0 && is_data {
            blocks.push(offset-1);
          } else if offset > 0 {
            cursors.push((offset-1,level+1));
          }
        }
        Ok((cursors,blocks))
      }
      fn bounds (points: &Vec<Self>) -> Option<Self::Bounds> {
        if points.is_empty() { return None }
        let pairs = ($({
          let optb: Option<($T,$T)> = Coord::bounds(
            points.iter().map(|p| { &p.$i }).collect()
          );
          match optb {
            None => { return None },
            Some(b) => b
          }
        }),+);
        let min = ($((pairs.$i).0),+);
        let max = ($((pairs.$i).1),+);
        Some((min,max))
      }
      fn bounds_to_range (bounds: Self::Bounds) -> Self::Range {
        ($(((bounds.0).$i,(bounds.1).$i)),+)
      }
      fn format_at (buf: &[u8], level: usize) -> Result<String,Error> {
        Ok(match level % Self::dim() {
          $($i => {
            let (_,p) = $T::from_bytes(buf)?;
            format!["{:?}", p]
          }),+
          _ => panic!("match case beyond dimension")
        })
      }
    }
  }
}

macro_rules! impl_comb {
  ($types:tt, ($H:ty,$($T:ty),*), $ix:tt, $dim:expr, ($($x:tt),*)) => {
    impl_comb!($types, ($($T),*), $ix, $dim, ($($x,)*$H));
    impl_comb!($types, ($($T),*), $ix, $dim, ($($x,)*($H,$H)));
  };
  ($types:tt, ($H:ty), $ix:tt, $dim:expr, ($($x:tt),*)) => {
    impl_point!($types, ($($x),*,$H), $ix, $dim);
    impl_point!($types, ($($x),*,($H,$H)), $ix, $dim);
  };
}

macro_rules! impl_dim {
  ($t:tt,$i:tt,$dim:expr) => {
    impl_comb![$t,$t,$i,$dim,()];
  }
}

#[cfg(feature="2d")] impl_dim![(A,B),(0,1),2];
#[cfg(feature="3d")] impl_dim![(A,B,C),(0,1,2),3];
#[cfg(feature="4d")] impl_dim![(A,B,C,D),(0,1,2,3),4];
#[cfg(feature="5d")] impl_dim![(A,B,C,D,E),(0,1,2,3,4),5];
#[cfg(feature="6d")] impl_dim![(A,B,C,D,E,F),(0,1,2,3,4,5),6];
#[cfg(feature="7d")] impl_dim![(A,B,C,D,E,F,G),(0,1,2,3,4,5,6),7];
#[cfg(feature="8d")] impl_dim![(A,B,C,D,E,F,G,H),(0,1,2,3,4,5,6,7),8];
