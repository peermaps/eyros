use desert::{ToBytes,CountBytes};
use crate::{Coord,Scalar,Value,tree::TreeRef,bytes::varint};
use failure::Error;
use std::collections::HashMap;

macro_rules! impl_to_bytes {
  ($Tree:ident, $Branch:ident, $Node:ident,
  $allocate:ident, $write_branch_bytes:ident, $write_point_bytes:ident, $write_data_bytes:ident,
  ($($i:tt),+),($($T:tt),+)) => {
    use crate::tree::{$Tree,$Branch,$Node};
    impl<$($T),+,V> ToBytes for $Tree<$($T),+,V> where $($T: Scalar),+, V: Value {
      fn to_bytes(&self) -> Result<Vec<u8>,Error> {
        let hsize = varint::length(self.count as u64)
          + self.bounds.count_bytes()
          + self.root.count_bytes();
        let (alloc,size) = $allocate(&self.root, hsize);
        let mut buf = vec![0u8;size];

        let mut offset = 0;
        offset += varint::encode(self.count as u64, &mut buf[offset..])?;
        offset += self.bounds.write_bytes(&mut buf[offset..])?;
 
        match self.root.as_ref() {
          $Node::Data(data) => {
            $write_data_bytes(data, &mut buf[offset..])?;
          },
          $Node::Branch(branch) => {
            //let (n,_) = alloc.get(&0).unwrap();
            //assert_eq![*n, hsize, "n ({}) != hsize ({})", *n, hsize];
            //assert_eq![*n, offset+4, "n ({}) != offset ({}+4)", *n, offset];
            offset += ((hsize*3+0) as u32).write_bytes(&mut buf[offset..])?;
            //assert_eq![offset, hsize, "offset ({}) != hsize ({})", offset, hsize];
            $write_branch_bytes(branch, &alloc, offset, &mut buf)?;
          },
          $Node::Ref(r) => {
            write_ref_bytes(*r, &mut buf[offset..])?;
          },
        }
        Ok(buf)
      }
    }

    fn $allocate<$($T),+,V>(root: &$Node<$($T),+,V>, hsize: usize) -> (HashMap<usize,(usize,usize)>,usize)
    where $($T: Scalar),+, V: Value {
      let mut alloc: HashMap<usize,(usize,usize)> = HashMap::new(); // index => (offset, size)
      let mut cursors = vec![root];
      let mut index = 0;
      let mut offset = hsize;
      while let Some(node) = cursors.pop() {
        match node {
          $Node::Data(_data) => {},
          $Node::Branch(branch) => {
            let size = branch.count_bytes();
            alloc.insert(index, (offset,size));
            offset += size;
            for b in branch.intersections.iter() {
              if let $Node::Branch(_br) = b.as_ref() {
                cursors.push(b);
              }
            }
            for b in branch.nodes.iter() {
              if let $Node::Branch(_br) = b.as_ref() {
                cursors.push(b);
              }
            }
            index += 1;
          },
          $Node::Ref(_r) => {}
        }
      }
      (alloc,offset)
    }

    fn $write_branch_bytes<$($T),+,V>(root: &$Branch<$($T),+,V>, alloc: &HashMap<usize,(usize,usize)>,
    i_offset: usize, buf: &mut [u8]) -> Result<usize,Error> where $($T: Scalar),+, V: Value {
      let mut cursors = vec![root];
      let mut offset = i_offset;
      let mut index = 0;
      while let Some(branch) = cursors.pop() {
        loop {
          $(if let Some(x) = &branch.pivots.$i {
            offset += x.write_bytes(&mut buf[offset..])?;
            break;
          })+
          panic!["pivots empty"];
        }
        let mut i = index + 1;
        for x in [&branch.intersections,&branch.nodes].iter() {
          for b in x.iter() {
            match b.as_ref() {
              $Node::Branch(branch) => {
                let (j,_) = alloc.get(&i).unwrap();
                offset += (((*j)*3+0) as u32).write_bytes(&mut buf[offset..])?;
                i += 1;
                cursors.push(branch);
              },
              $Node::Data(data) => {
                offset += $write_data_bytes(data, &mut buf[offset..])?;
              },
              $Node::Ref(r) => {
                offset += write_ref_bytes(*r, &mut buf[offset..])?;
              }
            }
          }
        }
        index += 1;
      }
      Ok(offset)
    }

    fn $write_point_bytes<$($T),+>(pt: &($(Coord<$T>),+), buf: &mut [u8]) -> Result<usize,Error>
    where $($T: Scalar),+ {
      let mut offset = 1;
      buf[0] = 0;
      $(match &pt.$i {
        Coord::Scalar(x) => {
          offset += x.write_bytes(&mut buf[offset..])?;
        },
        Coord::Interval(x,y) => {
          buf[0] |= 1 << $i;
          offset += x.write_bytes(&mut buf[offset..])?;
          offset += y.write_bytes(&mut buf[offset..])?;
        },
      })+
      Ok(offset)
    }

    fn $write_data_bytes<$($T),+,V>(rows: &Vec<(($(Coord<$T>),+),V)>, buf: &mut [u8]) -> Result<usize,Error>
    where $($T: Scalar),+, V: Value {
      let mut offset = 0;
      offset += ((rows.len()*3+1) as u32).write_bytes(&mut buf[offset..])?;
      for _j in 0..(rows.len()+7)/8 {
        buf[offset] = 0;
        offset += 1;
      }
      for row in rows.iter() {
        offset += $write_point_bytes(&row.0, &mut buf[offset..])?;
        offset += row.1.write_bytes(&mut buf[offset..])?;
      }
      Ok(offset)
    }
  }
}

fn write_ref_bytes(r: TreeRef, buf: &mut [u8]) -> Result<usize,Error> {
  ((r*3+2) as u32).write_bytes(buf)
}

impl_to_bytes![
  Tree2, Branch2, Node2,
  allocate2, write_branch_bytes2, write_point_bytes2, write_data_bytes2,
  (0,1), (P0,P1)
];
impl_to_bytes![
  Tree3, Branch3, Node3,
  allocate3, write_branch_bytes3, write_point_bytes3, write_data_bytes3,
  (0,1,2), (P0,P1,P2)
];
