use desert::FromBytes;
use crate::{Scalar,Coord,Value,bytes::varint,tree::TreeRef};
use failure::Error;
use async_std::sync::Arc;

macro_rules! impl_from_bytes {
  ($Tree:ident, $Branch:ident, $Node:ident,
  $parse_branch:ident, $parse_data:ident,
  ($($i:tt,$T:tt),+),($($n:tt),+),$dim:expr) => {
    use crate::tree::{$Tree,$Branch,$Node};
    impl<$($T),+,V> FromBytes for $Tree<$($T),+,V> where $($T: Scalar),+, V: Value {
      fn from_bytes(src: &[u8]) -> Result<(usize,Self),Error> {
        let mut offset = 0;
        let (s,n) = u32::from_bytes(&src[offset..])?;
        offset += s;
        let root = match n%2 {
          0 => {
            let root = $parse_branch(&src, (n/2) as usize, 0)?;
            root
          },
          1 => {
            let (s,data) = $parse_data(&src[offset..], n as usize)?;
            offset += s;
            data
          },
          _ => panic!["unexpected value for n%2: {}", n%2]
        };
        Ok((offset, $Tree {
          root: Arc::new(root),
        }))
      }
    }

    fn $parse_branch<$($T),+,V>(src: &[u8], xoffset: usize, depth: usize)
    -> Result<$Node<$($T),+,V>,Error> where $($T: Scalar),+, V: Value {
      let mut offset = xoffset;
      let mut pivots = ($($n),+);
      let pivot_len = match depth%$dim {
        $($i => {
          let (s,x) = <Vec<$T>>::from_bytes(&src[offset..])?;
          offset += s;
          let len = x.len();
          pivots.$i = Some(x);
          len
        },)+
        _ => panic!["unexpected modulo depth"]
      };
      let mut intersections = vec![];
      for _ in 0..pivot_len {
        let (s,n) = u32::from_bytes(&src[offset..])?;
        offset += s;
        match n%2 {
          0 => {
            intersections.push(Arc::new($parse_branch(&src, (n/2) as usize, depth+1)?));
          },
          1 => {
            let (s,data) = $parse_data(&src[offset..], n as usize)?;
            offset += s;
            intersections.push(Arc::new(data));
          },
          _ => panic!["unexpected value for n%2: {}", n%2]
        }
      }
      let mut nodes = vec![];
      for _ in 0..pivot_len+1 {
        let (s,n) = u32::from_bytes(&src[offset..])?;
        offset += s;
        match n%2 {
          0 => {
            nodes.push(Arc::new($parse_branch(&src, (n/2) as usize, depth+1)?));
          },
          1 => {
            let (s,data) = $parse_data(&src[offset..], n as usize)?;
            offset += s;
            nodes.push(Arc::new(data));
          },
          _ => panic!["unexpected value for n%2: {}", n%2]
        }
      }
      Ok($Node::Branch($Branch {
        pivots,
        intersections,
        nodes,
      }))
    }

    fn $parse_data<$($T),+,V>(src: &[u8], n: usize) -> Result<(usize,$Node<$($T),+,V>),Error>
    where $($T: Scalar),+, V: Value {
      let mut offset = 0;
      let (data_len,ref_len) = ((n>>1)&0xffff,n>>17);
      let mut data: Vec<(($(Coord<$T>),+),V)> = Vec::with_capacity(data_len);
      let mut refs: Vec<TreeRef<($(Coord<$T>),+)>> = vec![];
      let dbf = &src[offset..offset+(data_len+7)/8];
      offset += (data_len+7)/8;
      for i in 0..data_len {
        let bitfield = src[offset];
        offset += 1;
        let point = ($(
          match (bitfield>>$i)&1 {
            0 => {
              let (s,x) = $T::from_bytes(&src[offset..])?;
              offset += s;
              Coord::Scalar(x)
            },
            _ => {
              let (s,x) = $T::from_bytes(&src[offset..])?;
              offset += s;
              let (s,y) = $T::from_bytes(&src[offset..])?;
              offset += s;
              Coord::Interval(x,y)
            }
          }
        ),+);
        let (s,value) = V::from_bytes(&src[offset..])?;
        offset += s;
        let is_deleted = (dbf[i/8]>>(i%8))&1 == 1;
        if !is_deleted {
          data.push((point,value));
        }
      }
      for _i in 0..ref_len {
        let (s,r) = varint::decode(&src[offset..])?;
        offset += s;
        let tr = TreeRef {
          id: r,
          bounds: ($({
            let (s,xmin) = $T::from_bytes(&src[offset..])?;
            assert![xmin == xmin, "non-identity deserializing xmin={:?}", xmin];
            offset += s;
            let (s,xmax) = $T::from_bytes(&src[offset..])?;
            assert![xmax == xmax, "non-identity deserializing xmax={:?}", xmax];
            offset += s;
            Coord::Interval(xmin,xmax)
          }),+)
        };
        //eprintln!["from:bounds={:?}",&tr.bounds];
        refs.push(tr);
      }
      Ok((offset,$Node::Data(data,refs)))
    }
  }
}

#[cfg(feature="2d")] impl_from_bytes![
  Tree2,Branch2,Node2,parse_branch2,parse_data2,(0,P0,1,P1),(None,None),2
];
#[cfg(feature="3d")] impl_from_bytes![
  Tree3,Branch3,Node3,parse_branch3,parse_data3,(0,P0,1,P1,2,P2),(None,None,None),3
];
#[cfg(feature="4d")] impl_from_bytes![
  Tree4,Branch4,Node4,parse_branch4,parse_data4,(0,P0,1,P1,2,P2,3,P3),(None,None,None,None),4
];
#[cfg(feature="5d")] impl_from_bytes![
  Tree5,Branch5,Node5,parse_branch5,parse_data5,
  (0,P0,1,P1,2,P2,3,P3,4,P4),(None,None,None,None,None),5
];
#[cfg(feature="6d")] impl_from_bytes![
  Tree6,Branch6,Node6,parse_branch6,parse_data6,
  (0,P0,1,P1,2,P2,3,P3,4,P4,5,P5),(None,None,None,None,None,None),6
];
#[cfg(feature="7d")] impl_from_bytes![
  Tree7,Branch7,Node7,parse_branch7,parse_data7,
  (0,P0,1,P1,2,P2,3,P3,4,P4,5,P5,6,P6),(None,None,None,None,None,None,None),7
];
#[cfg(feature="8d")] impl_from_bytes![
  Tree8,Branch8,Node8,parse_branch8,parse_data8,
  (0,P0,1,P1,2,P2,3,P3,4,P4,5,P5,6,P6,7,P7),(None,None,None,None,None,None,None,None),8
];
