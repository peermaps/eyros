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
        let hsize = self.root.count_bytes();
        let (alloc,size) = $allocate(&self.root, hsize);
        let mut buf = vec![0u8;size];
        let mut offset = 0;
        match self.root.as_ref() {
          $Node::Data(data,refs) => {
            $write_data_bytes(data, refs, &mut buf[offset..])?;
          },
          $Node::Branch(branch) => {
            offset += ((hsize*2+0) as u32).write_bytes(&mut buf[offset..])?;
            $write_branch_bytes(branch, &alloc, offset, &mut buf)?;
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
      let mut cindex = 0;
      let mut offset = hsize;
      while let Some(node) = cursors.get(cindex) {
        match node {
          $Node::Data(_,_) => {},
          $Node::Branch(branch) => {
            let size = branch.count_bytes();
            alloc.insert(index, (offset,size));
            offset += size;
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
            index += 1;
          },
        }
        cindex += 1;
      }
      (alloc,offset)
    }

    fn $write_branch_bytes<$($T),+,V>(root: &$Branch<$($T),+,V>, alloc: &HashMap<usize,(usize,usize)>,
    i_offset: usize, buf: &mut [u8]) -> Result<usize,Error>
    where $($T: Scalar),+, V: Value {
      let mut cursors = vec![root];
      let mut offset = i_offset;
      let mut index = 0;
      let mut next_index = 1;
      while let Some(branch) = cursors.get(index) {
        let mut pivot_len = 0;
        loop {
          $(if let Some(x) = &branch.pivots.$i {
            pivot_len += x.len();
            offset += x.write_bytes(&mut buf[offset..])?;
            break;
          })+
          panic!["pivots empty"];
        }
        offset += varint::encode(branch.intersections.len() as u64, &mut buf[offset..])?;
        let ibitfield = &mut buf[offset..offset+(pivot_len*branch.intersections.len()+7)/8];
        ibitfield.fill(0);
        offset += ibitfield.len();
        {
          let mut i = 0;
          for (bitfield,_) in branch.intersections.iter() {
            for j in 0..pivot_len {
              ibitfield[i/8] |= (((bitfield>>j)&1) as u8)<<(i%8);
              i += 1;
            }
          }
        }
        let mut xcursors = vec![];
        for (_,b) in branch.intersections.iter() {
          match b.as_ref() {
            $Node::Branch(br) => {
              let (j,_size) = alloc.get(&next_index).unwrap();
              offset += (((*j)*2+0) as u32).write_bytes(&mut buf[offset..])?;
              next_index += 1;
              xcursors.push(br);
            },
            $Node::Data(data, refs) => {
              offset += $write_data_bytes(data, refs, &mut buf[offset..])?;
            },
          }
        }
        for b in branch.nodes.iter() {
          match b.as_ref() {
            $Node::Branch(br) => {
              let (j,_size) = alloc.get(&next_index).unwrap();
              offset += (((*j)*2+0) as u32).write_bytes(&mut buf[offset..])?;
              next_index += 1;
              xcursors.push(br);
            },
            $Node::Data(data, refs) => {
              offset += $write_data_bytes(data, refs, &mut buf[offset..])?;
            },
          }
        }
        cursors.extend(xcursors);
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

    fn $write_data_bytes<$($T),+,V>(rows: &[(($(Coord<$T>),+),V)],
    refs: &[TreeRef<($(Coord<$T>),+)>], buf: &mut [u8]) -> Result<usize,Error>
    where $($T: Scalar),+, V: Value {
      let mut offset = 0;
      let n = ((rows.len()<<1) + (refs.len()<<17) + 1) as u32;
      offset += n.write_bytes(&mut buf[offset..])?;
      for row in rows.iter() {
        offset += $write_point_bytes(&row.0, &mut buf[offset..])?;
        offset += row.1.write_bytes(&mut buf[offset..])?;
      }
      for r in refs.iter() {
        offset += varint::encode(r.id, &mut buf[offset..])?;
        $(match &r.bounds.$i {
          Coord::Interval(xmin,xmax) => {
            assert![xmin == xmin, "non-idenity serializing xmin={:?}", xmin];
            assert![xmax == xmax, "non-idenity serializing xmax={:?}", xmax];
            offset += xmin.write_bytes(&mut buf[offset..])?;
            offset += xmax.write_bytes(&mut buf[offset..])?;
          },
          _ => panic!["unexpected scalar in TreeRef bound"]
        };)+
      }
      Ok(offset)
    }
  }
}

#[cfg(feature="2d")] impl_to_bytes![
  Tree2, Branch2, Node2,
  allocate2, write_branch_bytes2, write_point_bytes2, write_data_bytes2,
  (0,1), (P0,P1)
];
#[cfg(feature="3d")] impl_to_bytes![
  Tree3, Branch3, Node3,
  allocate3, write_branch_bytes3, write_point_bytes3, write_data_bytes3,
  (0,1,2), (P0,P1,P2)
];
#[cfg(feature="4d")] impl_to_bytes![
  Tree4, Branch4, Node4,
  allocate4, write_branch_bytes4, write_point_bytes4, write_data_bytes4,
  (0,1,2,3), (P0,P1,P2,P3)
];
#[cfg(feature="5d")] impl_to_bytes![
  Tree5, Branch5, Node5,
  allocate5, write_branch_bytes5, write_point_bytes5, write_data_bytes5,
  (0,1,2,3,4), (P0,P1,P2,P3,P4)
];
#[cfg(feature="6d")] impl_to_bytes![
  Tree6, Branch6, Node6,
  allocate6, write_branch_bytes6, write_point_bytes6, write_data_bytes6,
  (0,1,2,3,4,5), (P0,P1,P2,P3,P4,P5)
];
#[cfg(feature="7d")] impl_to_bytes![
  Tree7, Branch7, Node7,
  allocate7, write_branch_bytes7, write_point_bytes7, write_data_bytes7,
  (0,1,2,3,4,5,6), (P0,P1,P2,P3,P4,P5,P6)
];
#[cfg(feature="8d")] impl_to_bytes![
  Tree8, Branch8, Node8,
  allocate8, write_branch_bytes8, write_point_bytes8, write_data_bytes8,
  (0,1,2,3,4,5,6,7), (P0,P1,P2,P3,P4,P5,P6,P7)
];
