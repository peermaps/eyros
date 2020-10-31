use desert::{ToBytes,CountBytes};
use crate::{Coord,Scalar,Value,tree::{Tree2,Branch2,Node2},bytes::varint};
use failure::Error;
use std::collections::HashMap;

impl<X,Y,V> ToBytes for Tree2<X,Y,V> where X: Scalar, Y: Scalar, V: Value {
  fn to_bytes(&self) -> Result<Vec<u8>,Error> {
    let hsize = varint::length(self.count as u64)
      + self.bounds.count_bytes()
      + self.root.count_bytes();
    let (alloc,size) = allocate(&self.root, hsize);
    let mut buf = vec![0u8;size];

    let mut offset = 0;
    offset += varint::encode(self.count as u64, &mut buf[offset..])?;
    offset += self.bounds.write_bytes(&mut buf[offset..])?;
    
    match &self.root {
      Node2::Data(data) => {
        write_data_bytes(data, &mut buf[offset..])?;
      },
      Node2::Branch(branch) => {
        //let (n,_) = alloc.get(&0).unwrap();
        //assert_eq![*n, hsize, "n ({}) != hsize ({})", *n, hsize];
        //assert_eq![*n, offset+4, "n ({}) != offset ({}+4)", *n, offset];
        offset += ((hsize*3+0) as u32).write_bytes(&mut buf[offset..])?;
        //assert_eq![offset, hsize, "offset ({}) != hsize ({})", offset, hsize];
        write_branch_bytes(branch, &alloc, offset, &mut buf)?;
      }
    }
    Ok(buf)
  }
}

fn allocate<X,Y,V>(root: &Node2<X,Y,V>, hsize: usize) -> (HashMap<usize,(usize,usize)>,usize)
where X: Scalar, Y: Scalar, V: Value {
  let mut alloc: HashMap<usize,(usize,usize)> = HashMap::new(); // index => (offset, size)
  let mut cursors = vec![root];
  let mut index = 0;
  let mut offset = hsize;
  while let Some(node) = cursors.pop() {
    match node {
      Node2::Data(_data) => {},
      Node2::Branch(branch) => {
        let size = branch.count_bytes();
        alloc.insert(index, (offset,size));
        offset += size;
        for b in branch.intersections.iter() {
          if let Node2::Branch(_br) = b {
            cursors.push(b);
          }
        }
        for b in branch.nodes.iter() {
          if let Node2::Branch(_br) = b {
            cursors.push(b);
          }
        }
        index += 1;
      }
    }
  }
  (alloc,offset)
}

fn write_branch_bytes<X,Y,V>(root: &Branch2<X,Y,V>, alloc: &HashMap<usize,(usize,usize)>,
i_offset: usize, buf: &mut [u8]) -> Result<usize,Error> where X: Scalar, Y: Scalar, V: Value {
  let mut cursors = vec![root];
  let mut offset = i_offset;
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

fn write_point_bytes<X,Y>(pt: &(Coord<X>,Coord<Y>), buf: &mut [u8]) -> Result<usize,Error>
where X: Scalar, Y: Scalar {
  let mut offset = 1;
  buf[0] = 0;
  match &pt.0 {
    Coord::Scalar(x) => {
      offset += x.write_bytes(&mut buf[offset..])?;
    },
    Coord::Interval(x,y) => {
      buf[0] |= 1 << 0;
      offset += x.write_bytes(&mut buf[offset..])?;
      offset += y.write_bytes(&mut buf[offset..])?;
    },
  }
  match &pt.1 {
    Coord::Scalar(x) => {
      offset += x.write_bytes(&mut buf[offset..])?;
    },
    Coord::Interval(x,y) => {
      buf[0] |= 1 << 1;
      offset += x.write_bytes(&mut buf[offset..])?;
      offset += y.write_bytes(&mut buf[offset..])?;
    },
  }
  Ok(offset)
}

fn write_data_bytes<X,Y,V>(rows: &Vec<((Coord<X>,Coord<Y>),V)>, buf: &mut [u8]) -> Result<usize,Error>
where X: Scalar, Y: Scalar, V: Value {
  let mut offset = 0;
  offset += ((rows.len()*3+1) as u32).write_bytes(&mut buf[offset..])?;
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
