use desert::{ToBytes,CountBytes,FromBytes};
use crate::{Coord,Scalar,Value,tree::{Tree2,Branch2,Node2}};
use failure::{Error,bail};
use std::collections::HashMap;
#[path="./varint.rs"] mod varint;

impl<X,Y,V> ToBytes for Tree2<X,Y,V> where X: Scalar, Y: Scalar, V: Value {
  fn to_bytes(&self) -> Result<Vec<u8>,Error> {
    match &self.root {
      Node2::Data(data) => {
        let mut offset = 0;
        let mut buf = vec![0u8;node_size(&self.root)];
        write_data_bytes(data, &mut buf)?;
        Ok(buf)
      },
      Node2::Branch(branch) => {
        let (mut alloc,size) = allocate(branch);
        let mut buf = vec![0u8;size];
        write_branch_bytes(branch, &mut alloc, &mut buf)?;
        Ok(buf)
      }
    }
  }
}

fn allocate<X,Y,V>(root: &Branch2<X,Y,V>) -> (HashMap<usize,(usize,usize)>,usize)
where X: Scalar, Y: Scalar, V: Value {
  let mut alloc: HashMap<usize,(usize,usize)> = HashMap::new(); // index => (offset, size)
  let mut cursors = vec![root];
  let mut index = 0;
  let mut offset = 0;
  while let Some(branch) = cursors.pop() {
    let size = r_size(branch);
    alloc.insert(index, (offset,size));
    offset += size;
    for b in branch.intersections.iter() {
      if let Node2::Branch(br) = b {
        cursors.push(br);
      }
    }
    for b in branch.nodes.iter() {
      if let Node2::Branch(br) = b {
        cursors.push(br);
      }
    }
    index += 1;
  }
  (alloc,offset)
}

fn write_branch_bytes<X,Y,V>(root: &Branch2<X,Y,V>, alloc: &mut HashMap<usize,(usize,usize)>,
buf: &mut [u8]) -> Result<usize,Error> where X: Scalar, Y: Scalar, V: Value {
  let mut cursors = vec![root];
  let mut offset = 0;
  let mut index = 0;
  while let Some(branch) = cursors.pop() {
    offset += match &branch.pivots {
      (Some(x),None) => x.write_bytes(&mut buf[offset..])?,
      (None,Some(x)) => x.write_bytes(&mut buf[offset..])?,
      _ => panic![""]
    };
    let mut i = index + 1;
    for x in [&branch.intersections,&branch.nodes].iter() {
      for b in x.iter() {
        match b {
          Node2::Branch(branch) => {
            let (j,_) = alloc.get(&i).unwrap();
            offset += (((*j)*3+0) as u32).write_bytes(&mut buf[offset..])?;
            i += 1;
            cursors.push(branch);
          },
          Node2::Data(data) => {
            offset += write_data_bytes(data, &mut buf[offset..])?;
          }
        }
      }
    }
    index += 1;
  }
  Ok(offset)
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

fn write_point_bytes<X,Y>(pt: &(Coord<X>,Coord<Y>), buf: &mut [u8]) -> Result<usize,Error>
where X: Scalar, Y: Scalar {
  let mut offset = 1;
  buf[0] = 0;
  buf[0] |= (match &pt.0 { Coord::Scalar(_) => 0, Coord::Interval(_,_) => 1 }) << 0;
  buf[0] |= (match &pt.1 { Coord::Scalar(_) => 0, Coord::Interval(_,_) => 1 }) << 1;
  match &pt.0 {
    Coord::Scalar(x) => {
      offset += x.write_bytes(&mut buf[offset..])?;
    },
    Coord::Interval(x,y) => {
      offset += x.write_bytes(&mut buf[offset..])?;
      offset += y.write_bytes(&mut buf[offset..])?;
    },
  }
  match &pt.1 {
    Coord::Scalar(x) => {
      offset += x.write_bytes(&mut buf[offset..])?;
    },
    Coord::Interval(x,y) => {
      offset += x.write_bytes(&mut buf[offset..])?;
      offset += y.write_bytes(&mut buf[offset..])?;
    },
  }
  Ok(offset)
}

fn write_data_bytes<X,Y,V>(rows: &Vec<((Coord<X>,Coord<Y>),V)>, buf: &mut [u8]) -> Result<usize,Error>
where X: Scalar, Y: Scalar, V: Value {
  let mut offset = 0;
  offset += varint::encode((rows.len()*3+1) as u64, &mut buf[offset..])?;
  for _j in 0..(rows.len()+7)/8 {
    buf[offset] = 0;
    offset += 1;
  }
  for row in rows.iter() {
    offset += write_point_bytes(&row.0, &mut buf[offset..])?;
    offset += row.1.write_bytes(&mut buf[offset..])?;
  }
  Ok(offset)
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

fn node_size<X,Y,V>(node: &Node2<X,Y,V>) -> usize where X: Scalar, Y: Scalar, V: Value {
  match node {
    Node2::Branch(branch) => 4,
    Node2::Data(rows) => varint::length((rows.len() as u64)*3+1)
      + (rows.len()+7)/8 // deleted bitfield
      + rows.iter().fold(0usize, |sum,row| {
        sum + count_point_bytes(&row.0) + row.1.count_bytes()
      }),
  }
}
