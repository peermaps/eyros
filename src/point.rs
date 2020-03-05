use std::cmp::Ordering;
use std::ops::{Div,Add};
use failure::Error;
use serde::{Serialize,de::DeserializeOwned};
use std::fmt::Debug;
use std::mem::size_of;
use crate::take_bytes::TakeBytes;

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

pub trait Point: Copy+Clone+Debug+TakeBytes+Serialize+DeserializeOwned {
  /// Bounding-box corresponding to `(min,max)` as used by `db.query(bbox)`.
  type Bounds: Copy+Clone+Debug+Serialize+DeserializeOwned;

  /// Range corresponding to `((minX,maxX),(minY,maxY),...)`
  type Range: Point+Copy+Clone+Debug+Serialize+DeserializeOwned;

  /// Compare elements at a level of tree depth. The dimension under
  /// consideration alternates each level, so you'll likely want the element
  /// at an index corresponding to `level % dimension`.
  fn cmp_at (&self, other: &Self, level: usize) -> Ordering where Self: Sized;

  /// Determine whether an element in a buffer slice greater than the bbox
  /// minimum and less than the bbox maximum for a particular tree depth.
  /// Each of these comparisons `(isGTMin,isLTMax)` is returned in the result
  /// type. A bincode configuration is required to avoid parsing the entire
  /// buffer where possible, as only a single element is needed.
  fn cmp_buf (bincode: &bincode::Config, buf: &[u8], bbox: &Self::Bounds,
    level: usize) -> Result<(bool,bool),Error>;

  /// For intervals, calculate the midpoint of the greater (upper) interval
  /// bound (ex: `iv.1`) for two intervals, returning a new interval where both
  /// elements are the midpoint result.
  /// For scalars, return the midpoint of two scalars as a scalar.
  fn midpoint_upper (&self, other: &Self) -> Self where Self: Sized;

  /// Return the byte presentation for the element corresponding to the tree
  /// depth `level` for the purpose of making a pivot. If you have an interval
  /// type, return the upper bound.
  fn serialize_at (&self, bincode: &bincode::Config, level: usize)
    -> Result<Vec<u8>,Error>;

  /// Get the number of dimensions for this point type.
  fn dim () -> usize;

  /// Return whether the current point intersects with a bounding box.
  fn overlaps (&self, bbox: &Self::Bounds) -> bool;

  /// Return the size in bytes of the element corresponding to the tree depth
  /// `level`.
  // TODO: self
  fn pivot_size_at (level: usize) -> usize;

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
  fn format_at (bincode: &bincode::Config, buf: &[u8], level: usize)
    -> Result<String,Error>;
}

pub trait Num<T>: PartialOrd+Copy+Serialize+DeserializeOwned
+Debug+Scalar+From<u8>+Div<T,Output=T>+Add<T,Output=T> {}
impl<T> Num<T> for T where T: PartialOrd+Copy+Serialize+DeserializeOwned
+Debug+Scalar+From<u8>+Div<T,Output=T>+Add<T,Output=T> {}

/// Types representing a single value (as opposed to an interval, which has
/// minimum and maximum values).
///
/// This trait has no required methods.
pub trait Scalar: Copy+Sized+'static {}
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
    let x = (self.1 + other.1) / 2.into();
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
      fn cmp_buf (bincode: &bincode::Config, buf: &[u8], bbox: &Self::Bounds,
      level: usize) -> Result<(bool,bool),Error> {
        match level % $dim {
          $($i => {
            let point: $T = bincode.deserialize(buf)?;
            Ok((
              (bbox.0).$i <= point,
              point <= (bbox.1).$i
            ))
          },)+
          _ => panic!("level out of bounds")
        }
      }
      fn midpoint_upper (&self, other: &Self) -> Self {
        ($(
          Coord::midpoint_upper(&self.$i, &other.$i)
        ),+)
      }
      fn serialize_at (&self, bincode: &bincode::Config, level: usize)
      -> Result<Vec<u8>,Error> {
        let buf: Vec<u8> = match level%Self::dim() {
          $($i => bincode.serialize(&self.$i.upper())?,)+
          _ => panic!("match case beyond dimension")
        };
        Ok(buf)
      }
      fn dim () -> usize { $dim }
      fn overlaps (&self, bbox: &Self::Bounds) -> bool {
        $(Coord::overlaps(&self.$i, &(bbox.0).$i, &(bbox.1).$i) &&)+ true
      }
      fn pivot_size_at (i: usize) -> usize {
        match i % $dim {
          $($i => size_of::<$T>(),)+
          _ => panic!("dimension out of bounds")
        }
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
      fn format_at (bincode: &bincode::Config, buf: &[u8], level: usize)
      -> Result<String,Error> {
        Ok(match level % Self::dim() {
          $($i => {
            let p: $T = bincode.deserialize(buf)?;
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

impl_dim![(A,B),(0,1),2];
impl_dim![(A,B,C),(0,1,2),3];
impl_dim![(A,B,C,D),(0,1,2,3),4];
//impl_dim![(A,B,C,D,E),(0,1,2,3,4),5];
//impl_dim![(A,B,C,D,E,F),(0,1,2,3,4,5),6];
//impl_dim![(A,B,C,D,E,F,G),(0,1,2,3,4,5,6),7];
//impl_dim![(A,B,C,D,E,F,G,H),(0,1,2,3,4,5,6,7),8];
