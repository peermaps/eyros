use desert::{CountBytes,varint};
use crate::{Coord,Scalar,Value,Error};

macro_rules! impl_count_bytes {
  ($Tree:ident,$Branch:ident,$Node:ident,$count_point_bytes:ident,($($i:tt),+),($($T:tt),+)) => {
    use crate::tree::{$Tree,$Branch,$Node};

    impl<$($T),+,V> CountBytes for $Tree<$($T),+,V> where $($T: Scalar),+, V: Value {
      fn count_bytes(&self) -> usize {
        let mut bytes = self.root.count_bytes();
        let mut cursors = vec![&*self.root];
        while let Some(node) = cursors.pop() {
          match &*node {
            $Node::Branch(branch) => {
              bytes += branch.count_bytes();
              for (_,b) in branch.intersections.iter() {
                if let $Node::Branch(_br) = b.as_ref() {
                  cursors.push(b);
                }
              }
              for b in branch.nodes.iter() {
                if let $Node::Branch(_br) = b.as_ref() {
                  cursors.push(b);
                }
              }
            },
            _ => {},
          }
        }
        bytes
      }
      fn count_from_bytes(_src: &[u8]) -> Result<usize,Error> {
        unimplemented![]
      }
    }

    impl<$($T),+,V> CountBytes for $Branch<$($T),+,V> where $($T: Scalar),+, V: Value {
      fn count_bytes(&self) -> usize {
        let mut size = 0;
        let mut pivot_len = 0;
        loop {
          $(if let Some(x) = &self.pivots.$i {
            pivot_len += x.len();
            size += x.count_bytes();
            break;
          })+
          panic!["invalid pivot state"] // todo: use a custom enum for pivots
        }
        size += varint::length(self.intersections.len() as u64);
        size += (self.intersections.len()*pivot_len+7)/8;
        for (_,b) in self.intersections.iter() {
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
          $Node::Data(rows,refs) => 4
            + rows.iter().fold(0usize, |sum,row| {
              sum + $count_point_bytes(&row.0) + row.1.count_bytes()
            })
            + refs.iter().fold(0usize, |sum,r| {
              sum + varint::length(r.id as u64)
                $(+ match &r.bounds.$i {
                  Coord::Interval(x,y) => x.count_bytes() + y.count_bytes(),
                  _ => panic!["unexpected scalar in TreeRef bound"],
                })+
            }),
        }
      }
      fn count_from_bytes(_src: &[u8]) -> Result<usize,Error> {
        unimplemented![]
      }
    }

    pub fn $count_point_bytes<$($T),+>(pt: &($(Coord<$T>),+)) -> usize where $($T: Scalar),+ {
      let mut size = 1; // 1-byte arity bitfield
      $(size += match &pt.$i {
        Coord::Scalar(x) => x.count_bytes(),
        Coord::Interval(x,y) => x.count_bytes() + y.count_bytes(),
      };)+
      size
    }
  }
}

#[cfg(feature="2d")] impl_count_bytes![
  Tree2,Branch2,Node2,count_point_bytes2,(0,1),(P0,P1)
];
#[cfg(feature="3d")] impl_count_bytes![
  Tree3,Branch3,Node3,count_point_bytes3,(0,1,2),(P0,P1,P2)
];
#[cfg(feature="4d")] impl_count_bytes![
  Tree4,Branch4,Node4,count_point_bytes4,(0,1,2,3),(P0,P1,P2,P3)
];
#[cfg(feature="5d")] impl_count_bytes![
  Tree5,Branch5,Node5,count_point_bytes5,(0,1,2,3,4),(P0,P1,P2,P3,P4)
];
#[cfg(feature="6d")] impl_count_bytes![
  Tree6,Branch6,Node6,count_point_bytes6,(0,1,2,3,4,5),(P0,P1,P2,P3,P4,P5)
];
#[cfg(feature="7d")] impl_count_bytes![
  Tree7,Branch7,Node7,count_point_bytes7,(0,1,2,3,4,5,6),(P0,P1,P2,P3,P4,P5,P6)
];
#[cfg(feature="8d")] impl_count_bytes![
  Tree8,Branch8,Node8,count_point_bytes8,(0,1,2,3,4,5,6,7),(P0,P1,P2,P3,P4,P5,P6,P7)
];
