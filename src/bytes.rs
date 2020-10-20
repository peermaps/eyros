use desert::{ToBytes,CountBytes,FromBytes};
use crate::{Coord,Scalar,Value,tree::{Tree2,Branch2,Node2}};
use failure::{Error,bail};
use std::collections::HashMap;
#[path="./varint.rs"] mod varint;

impl<X,Y,V> ToBytes for Tree2<X,Y,V> where X: Scalar, Y: Scalar, V: Value {
  fn to_bytes(&self) -> Result<Vec<u8>,Error> {
    let mut offsets: HashMap<usize,usize> = HashMap::new();
    let mut sizes: HashMap<usize,usize> = HashMap::new();

    let mut cursors = vec![(&self.root,0usize)];
    let mut index = 0;
    let mut offset = 0;
    while let Some((c,depth)) = cursors.pop() {
      match c {
        Node2::Branch(branch) => {
          let size = r_size(branch);
          sizes.insert(index, size);
          offsets.insert(index, offset);
          offset += size;
          for b in branch.intersections.iter() {
            cursors.push((b,depth+1));
          }
          for b in branch.nodes.iter() {
            cursors.push((b,depth+1));
          }
          index += 1;
        },
        Node2::Data(data) => {},
      }
    }
    eprintln!["offsets={:?}", offsets];
    eprintln!["sizes={:?}", sizes];
    unimplemented![]
  }
}

fn point_bytes<X,Y>(pt: &(Coord<X>,Coord<Y>)) -> usize where X: Scalar, Y: Scalar {
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

fn node_size<X,Y,V>(node: &Node2<X,Y,V>) -> usize where X: Scalar, Y: Scalar, V: Value {
  match node {
    Node2::Branch(branch) => varint::length((r_size(branch) as u64)*3+0),
    Node2::Data(rows) => varint::length((rows.len() as u64)*3+1)
      + (rows.len()+7)/8 // deleted bitfield
      + rows.iter().fold(0usize, |sum,row| sum + point_bytes(&row.0) + row.1.count_bytes()),
  }
}

fn r_size<X,Y,V>(branch: &Branch2<X,Y,V>) -> usize where X: Scalar, Y: Scalar, V: Value {
  let mut size = 0;
  size += match &branch.pivots {
    (Some(x),None) => x.count_bytes(),
    (None,Some(x)) => x.count_bytes(),
    _ => panic!["invalid pivot state"] // todo: use a custom enum for pivots
  };
  for b in branch.intersections.iter() {
    size += node_size(&b);
  }
  for b in branch.nodes.iter() {
    size += node_size(&b);
  }
  size
}
