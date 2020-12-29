use desert::CountBytes;
use crate::{Coord,Scalar,Value};
use failure::Error;

macro_rules! impl_count_bytes {
  ($Branch:ident,$Node:ident,$count_point_bytes:ident,($($i:tt),+),($($T:tt),+)) => {
    use crate::tree::{$Branch,$Node};
    impl<$($T),+,V> CountBytes for $Branch<$($T),+,V> where $($T: Scalar),+, V: Value {
      fn count_bytes(&self) -> usize {
        let mut size = 0;
        loop {
          $(if let Some(x) = &self.pivots.$i {
            size += x.count_bytes();
            break;
          })+
          panic!["invalid pivot state"] // todo: use a custom enum for pivots
        }
        for b in self.intersections.iter() {
          size += b.count_bytes();
        }
        for b in self.nodes.iter() {
          size += b.count_bytes();
        }
        size
      }
      fn count_from_bytes(_src: &[u8]) -> Result<usize,Error> {
        unimplemented![]
      }
    }

    impl<$($T),+,V> CountBytes for $Node<$($T),+,V> where $($T: Scalar),+, V: Value {
      fn count_bytes(&self) -> usize {
        match &self {
          $Node::Branch(_branch) => 4,
          $Node::Data(rows) => 4
            + (rows.len()+7)/8 // deleted bitfield
            + rows.iter().fold(0usize, |sum,row| {
              sum + $count_point_bytes(&row.0) + row.1.count_bytes()
            }),
          $Node::Ref(_r) => 4,
        }
      }
      fn count_from_bytes(_src: &[u8]) -> Result<usize,Error> {
        unimplemented![]
      }
    }

    fn $count_point_bytes<$($T),+>(pt: &($(Coord<$T>),+)) -> usize where $($T: Scalar),+ {
      let mut size = 1; // 1-byte arity bitfield
      $(size += match &pt.$i {
        Coord::Scalar(x) => x.count_bytes(),
        Coord::Interval(x,y) => x.count_bytes() + y.count_bytes(),
      };)+
      size
    }
  }
}

impl_count_bytes![Branch2,Node2,count_point_bytes2,(0,1),(P0,P1)];
impl_count_bytes![Branch3,Node3,count_point_bytes3,(0,1,2),(P0,P1,P2)];
