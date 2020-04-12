use crate::{Point,Cursor,Block,order,order_len};
use failure::{Error,bail};
use std::mem::size_of;

use std::cmp::{Ordering,PartialOrd};
use std::ops::{Add,Div};
use desert::{FromBytes,ToBytes,CountBytes};
use std::fmt::Debug;

#[derive(Copy,Clone,Debug,Eq,PartialEq)]
pub enum Mix<T> {
  Scalar(T),
  Interval(T,T)
}

macro_rules! impl_mix {
  ($M:ident,$dim:expr,($($T:tt),+),($($v:tt),+),($($i:tt),+)) => {
    #[derive(Copy,Clone,Debug,Eq,PartialEq)]
    pub struct $M<$($T),+> {
      $(pub $v: Mix<$T>,)+
    }
    impl<$($T),+> $M<$($T),+> {
      pub fn new($($v: Mix<$T>),+) -> Self {
        Self { $($v),+ }
      }
    }

    impl<$($T),+> CountBytes for $M<$($T),+> where $($T: CountBytes),+ {
      fn count_bytes(&self) -> usize {
        1 $(+ match &self.$v {
          Mix::Scalar(x) => x.count_bytes(),
          Mix::Interval(x0,x1) => x0.count_bytes() + x1.count_bytes(),
        })+
      }
      fn count_from_bytes(buf: &[u8]) -> Result<usize,Error> {
        if buf.len() < 1 { bail!["buffer too small for type in count"] }
        let mut offset = 1;
        $(if ((buf[0]>>$i)&1) == 0 {
          offset += $T::count_from_bytes(&buf[offset..])?;
        } else {
          offset += $T::count_from_bytes(&buf[offset..])?;
          offset += $T::count_from_bytes(&buf[offset..])?;
        })+
        Ok(offset)
      }
    }

    impl<$($T),+> ToBytes for $M<$($T),+> where $($T: ToBytes+CountBytes),+ {
      fn to_bytes(&self) -> Result<Vec<u8>,Error> {
        let count = self.count_bytes();
        let mut bytes = vec![0u8;count];
        let size = self.write_bytes(&mut bytes)?;
        if size != count { bail!["unexpected size while writing into buffer"] }
        Ok(bytes)
      }
      fn write_bytes(&self, dst: &mut [u8]) -> Result<usize,Error> {
        if dst.len() < 1 { bail!["dst buffer too small"] }
        let mut offset = 1;
        dst[0] = 0;
        $(match &self.$v {
          Mix::Scalar(x) => {
            offset += x.write_bytes(&mut dst[offset..])?;
          },
          Mix::Interval(x0,x1) => {
            dst[0] |= 1 << $i;
            offset += x0.write_bytes(&mut dst[offset..])?;
            offset += x1.write_bytes(&mut dst[offset..])?;
          }
        })+
        Ok(offset)
      }
    }

    impl<$($T),+> FromBytes for $M<$($T),+> where $($T: FromBytes),+ {
      fn from_bytes(src: &[u8]) -> Result<(usize,Self),Error> {
        if src.len() < 1 {
          bail!["buffer too small while loading from bytes"]
        }
        let mut offset = 1;
        $(let $v = if (src[0]>>$i)&1 == 0 {
          let (size,x) = $T::from_bytes(&src[offset..])?;
          offset += size;
          Mix::Scalar(x)
        } else {
          let (size0,x0) = $T::from_bytes(&src[offset..])?;
          offset += size0;
          let (size1,x1) = $T::from_bytes(&src[offset..])?;
          offset += size1;
          Mix::Interval(x0,x1)
        };)+
        Ok((offset, $M { $($v),+ }))
      }
    }

    impl<$($T),+> Point for $M<$($T),+> where ($(($T,$T)),+): Point,
    $($T: ToBytes+FromBytes+CountBytes+Copy+Debug+PartialOrd
    +Add<Output=$T>+Div<Output=$T>+From<u8>),+ {
      type Bounds = (($($T),+),($($T),+));
      type Range = ($(($T,$T)),+);

      fn cmp_at (&self, other: &Self, level: usize) -> Ordering where Self: Sized {
        let order = match level % Self::dim() {
          $($i => {
            match (self.$v, other.$v) {
              (Mix::Scalar(a),Mix::Scalar(b)) => a.partial_cmp(&b),
              (Mix::Interval(a0,a1),Mix::Scalar(b)) => {
                if b >= a0 && b <= a1 {
                  Some(Ordering::Equal)
                } else {
                  a0.partial_cmp(&b)
                }
              },
              (Mix::Scalar(a),Mix::Interval(b0,b1)) => {
                if a >= b0 && a <= b1 {
                  Some(Ordering::Equal)
                } else {
                  b0.partial_cmp(&a)
                }
              },
              (Mix::Interval(a0,a1),Mix::Interval(b0,b1)) => {
                if a0 <= b1 && b0 <= a1 {
                  Some(Ordering::Equal)
                } else {
                  a0.partial_cmp(&b0)
                }
              },
            }
          },)+
          _ => panic!["match case beyond dimension"]
        };
        match order { Some(x) => x, None => Ordering::Less }
      }

      fn midpoint_upper (&self, other: &Self) -> Self where Self: Sized {
        $(let $v = Mix::Scalar(match (self.$v, other.$v) {
          (Mix::Scalar(a),Mix::Scalar(b)) => (a+b)/2.into(),
          (Mix::Interval(_,a),Mix::Scalar(b)) => (a+b)/2.into(),
          (Mix::Scalar(a),Mix::Interval(_,b)) => (a+b)/2.into(),
          (Mix::Interval(_,a),Mix::Interval(_,b)) => (a+b)/2.into(),
        });)+
        Self { $($v),+ }
      }

      fn serialize_at (&self, level: usize, dst: &mut [u8]) -> Result<usize,Error> {
        match level % Self::dim() {
          $($i => match self.$v {
            Mix::Scalar(x) => x.write_bytes(dst),
            Mix::Interval(_,x) => x.write_bytes(dst),
          }),+
          _ => panic!["match case beyond dimension"]
        }
      }

      fn dim () -> usize { 2 }

      fn overlaps (&self, bbox: &Self::Bounds) -> bool {
        true $(&& (match self.$v {
          Mix::Scalar(x) => (bbox.0).$i <= x && x <= (bbox.1).$i,
          Mix::Interval(x0,x1) => (bbox.0).$i <= x1 && x0 <= (bbox.1).$i
        }))+
      }

      fn query_branch (buf: &[u8], bbox: &Self::Bounds, bf: usize, level: usize)
      -> Result<(Vec<Cursor>,Vec<Block>),Error> {
        let mut cursors = vec![];
        let mut blocks = vec![];
        let n = order_len(bf);
        let dim = level % Self::dim();
        let mut pivots: ($(Vec<$T>),+) = ($({ let v: Vec<$T> = vec![]; v }),+);
        let mut offset = 0;
        for _i in 0..n {
          match dim {
            $($i => {
              let (size,pivot) = $T::from_bytes(&buf[offset..])?;
              pivots.$i.push(pivot);
              offset += size;
            },)+
            _ => panic!["dimension not expected"]
          }
        }
        let d_start = offset; // data bitfield
        let i_start = d_start + (n+bf+7)/8; // intersections
        let b_start = i_start + n*size_of::<u64>(); // buckets

        let mut bcursors = vec![0];
        let mut bitfield: Vec<bool> = vec![false;bf]; // which buckets
        while !bcursors.is_empty() {
          let c = bcursors.pop().unwrap();
          let i = order(bf, c);
          let cmp = match dim {
            $($i => {
              let pivot = pivots.$i[i];
              ((bbox.0).$i <= pivot, pivot <= (bbox.1).$i)
            },)+
            _ => panic!["dimension not expected"]
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

      fn pivot_bytes_at (&self, level: usize) -> usize {
        match level % Self::dim() {
          $($i => match self.$v {
            Mix::Scalar(x) => x.count_bytes(),
            Mix::Interval(_,x) => x.count_bytes(),
          },)+
          _ => panic!["dimension not expected"]
        }
      }

      fn count_bytes_at (buf: &[u8], level: usize) -> Result<usize,Error> {
        Ok(match level % Self::dim() {
          $($i => $T::count_from_bytes(buf)?,)+
          _ => panic!["dimension not expected"]
        })
      }

      fn bounds (points: &Vec<Self>) -> Option<Self::Bounds> {
        if points.is_empty() { return None }
        fn lower<T> (x: &Mix<T>) -> &T {
          match x {
            Mix::Scalar(x) => x,
            Mix::Interval(x,_) => x
          }
        }
        fn upper<T> (x: &Mix<T>) -> &T {
          match x {
            Mix::Scalar(x) => x,
            Mix::Interval(_,x) => x
          }
        }
        let mut iter = points.iter();
        let first = iter.next()?;
        let mut bbox = (
          ($(*lower(&first.$v)),+),
          ($(*upper(&first.$v)),+),
        );
        for m in iter {
          $({
            let l = *lower(&m.$v);
            if l < (bbox.0).$i {
              (bbox.0).$i = l;
            }
            let u = *upper(&m.$v);
            if u > (bbox.1).$i {
              (bbox.1).$i = u;
            }
          })+
        }
        Some(bbox)
      }

      fn bounds_to_range (bbox: Self::Bounds) -> Self::Range {
        ($(((bbox.0).$i,(bbox.1).$i)),+)
      }

      fn format_at (_buf: &[u8], _level: usize)
      -> Result<String,Error> {
        unimplemented![]
      }
    }
  }
}

impl_mix![Mix2,2,(A,B),(v0,v1),(0,1)];
impl_mix![Mix3,3,(A,B,C),(v0,v1,v2),(0,1,2)];
impl_mix![Mix4,4,(A,B,C,D),(v0,v1,v2,v3),(0,1,2,3)];
impl_mix![Mix5,5,(A,B,C,D,E),(v0,v1,v2,v3,v4),(0,1,2,3,4)];
impl_mix![Mix6,6,(A,B,C,D,E,F),(v0,v1,v2,v3,v4,v5),(0,1,2,3,4,5)];
impl_mix![Mix7,7,(A,B,C,D,E,F,G),(v0,v1,v2,v3,v4,v5,v6),(0,1,2,3,4,5,6)];
impl_mix![Mix8,8,(A,B,C,D,E,F,G,H),(v0,v1,v2,v3,v4,v5,v6,v7),(0,1,2,3,4,5,6,7)];
