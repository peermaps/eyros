use desert::CountBytes;
use crate::{Coord,Scalar,Value,tree::{Branch2,Node2}};
use failure::Error;

impl<X,Y,V> CountBytes for Branch2<X,Y,V> where X: Scalar, Y: Scalar, V: Value {
  fn count_bytes(&self) -> usize {
    let mut size = 0;
    size += match &self.pivots {
      (Some(x),None) => x.count_bytes(),
      (None,Some(x)) => x.count_bytes(),
      _ => panic!["invalid pivot state"] // todo: use a custom enum for pivots
    };
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

impl<X,Y,V> CountBytes for Node2<X,Y,V> where X: Scalar, Y: Scalar, V: Value {
  fn count_bytes(&self) -> usize {
    match &self {
      Node2::Branch(_branch) => 4,
      Node2::Data(rows) => 4
        + (rows.len()+7)/8 // deleted bitfield
        + rows.iter().fold(0usize, |sum,row| {
          sum + count_point_bytes(&row.0) + row.1.count_bytes()
        }),
      Node2::Ref(_r) => 4,
    }
  }
  fn count_from_bytes(_src: &[u8]) -> Result<usize,Error> {
    unimplemented![]
  }
}

fn count_point_bytes<X,Y>(pt: &(Coord<X>,Coord<Y>)) -> usize where X: Scalar, Y: Scalar {
  let mut size = 1; // 1-byte arity bitfield
  size += match &pt.0 {
    Coord::Scalar(x) => x.count_bytes(),
    Coord::Interval(x,y) => x.count_bytes() + y.count_bytes(),
  };
  size += match &pt.1 {
    Coord::Scalar(x) => x.count_bytes(),
    Coord::Interval(x,y) => x.count_bytes() + y.count_bytes(),
  };
  size
}
