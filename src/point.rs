use std::cmp::Ordering;
use std::ops::{Div,Add};
use failure::Error;
use bincode::{serialize,deserialize};
use serde::{Serialize,de::DeserializeOwned};
use std::fmt::Debug;
use std::mem::size_of;

pub trait Point: Copy+Clone+Debug+Serialize+DeserializeOwned {
  type BBox;
  fn cmp_at (&self, &Self, usize) -> Ordering where Self: Sized;
  fn cmp_buf (&[u8], &Self::BBox, usize) -> Result<(bool,bool),Error>;
  fn midpoint_upper (&self, &Self) -> Self where Self: Sized;
  fn serialize_at (&self, usize) -> Result<Vec<u8>,Error>;
  fn dim () -> usize;
  fn overlaps (&self, &Self::BBox) -> bool;
  fn pivot_size_at (usize) -> usize;
}

pub trait Num<T>: PartialOrd+Copy+Serialize+DeserializeOwned
+Debug+Scalar+From<u8>+Div<T,Output=T>+Add<T,Output=T> {}
impl<T> Num<T> for T where T: PartialOrd+Copy+Serialize+DeserializeOwned
+Debug+Scalar+From<u8>+Div<T,Output=T>+Add<T,Output=T> {}

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
  fn cmp (&self, &Self) -> Option<Ordering>;
  fn midpoint_upper (&self, &Self) -> Self;
  fn upper (&self) -> T;
  fn overlaps (&self, &T, &T) -> bool;
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
    self.0.overlaps(min,max) || self.1.overlaps(min,max)
    || min.overlaps(&self.0, &self.1) || max.overlaps(&self.0, &self.1)
  }
}

macro_rules! impl_point {
  (($($T:tt),+),($($U:tt),+),($($i:tt),+),$dim:expr) => {
    impl<$($T),+> Point for ($($U),+)
    where $($T: Num<$T>),+ {
      type BBox = (($($T,)+),($($T,)+));
      fn cmp_at (&self, other: &Self, level: usize) -> Ordering {
        let order = match level%Self::dim() {
          $($i => Coord::cmp(&self.$i, &other.$i),)+
          _ => panic!("match case beyond dimension")
        };
        match order { Some(x) => x, None => Ordering::Less }
      }
      fn cmp_buf (buf: &[u8], bbox: &Self::BBox, level: usize)
      -> Result<(bool,bool),Error> {
        match level % $dim {
          $($i => {
            let point: $T = deserialize(&buf)?;
            Ok((
              (bbox.0).$i <= point,
             (bbox.1).$i >= point
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
      fn serialize_at (&self, level: usize) -> Result<Vec<u8>,Error> {
        let buf: Vec<u8> = match level%Self::dim() {
          $($i => serialize(&self.$i.upper())?,)+
          _ => panic!("match case beyond dimension")
        };
        Ok(buf)
      }
      fn dim () -> usize { $dim }
      fn overlaps (&self, bbox: &Self::BBox) -> bool {
        $(Coord::overlaps(&self.$i, &(bbox.0).$i, &(bbox.1).$i) &&)+ true
      }
      fn pivot_size_at (i: usize) -> usize {
        match i % $dim {
          $($i => size_of::<$T>(),)+
          _ => panic!("dimension out of bounds")
        }
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