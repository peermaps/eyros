use std::cmp::Ordering;
use std::ops::{Div,Add};
use std::mem::transmute;
use failure::Error;
use bincode::{serialize,deserialize};
use serde::{Serialize,de::DeserializeOwned};

pub trait Point {
  fn cmp_at (&self, &Self, usize) -> Ordering where Self: Sized;
  fn midpoint_upper (&self, &Self) -> Self where Self: Sized;
  fn serialize (&self) -> Result<Vec<u8>,Error>;
  fn serialize_at (&self, usize) -> Result<Vec<u8>,Error>;
  fn deserialize (&[u8]) -> Result<Self,Error> where Self: Sized;
}

impl<A,B> Point for ((A,A),(B,B)) where
A: PartialOrd+Copy+Serialize+DeserializeOwned+From<u8>+Div<A,Output=A>+Add<A,Output=A>,
B: PartialOrd+Copy+Serialize+DeserializeOwned+From<u8>+Div<B,Output=B>+Add<B,Output=B> {
  fn cmp_at (&self, other: &Self, level: usize) -> Ordering {
    let order = match level%2 {
      0 => if (self.0).0 <= (other.0).1 && (other.0).0 <= (self.0).1
        { Some(Ordering::Equal) } else { (self.0).0.partial_cmp(&(other.0).0) },
      _ => if (self.1).0 <= (other.1).1 && (other.1).0 <= (self.1).1
        { Some(Ordering::Equal) } else { (self.1).0.partial_cmp(&(other.1).0) }
    };
    match order { Some(x) => x, None => Ordering::Less }
  }
  fn midpoint_upper (&self, other: &Self) -> Self {
    let a = ((self.0).1 + (other.0).1) / 2.into();
    let b = ((self.1).1 + (other.1).1) / 2.into();
    ((a,a),(b,b))
  }
  fn serialize (&self) -> Result<Vec<u8>,Error> {
    let buf: Vec<u8> = serialize(self)?;
    Ok(buf)
  }
  fn serialize_at (&self, level: usize) -> Result<Vec<u8>,Error> {
    let buf: Vec<u8> = match level%2 {
      0 => serialize(&self.0)?,
      _ => serialize(&self.1)?
    };
    Ok(buf)
  }
  fn deserialize (data: &[u8]) -> Result<Self,Error> {
    let pt = deserialize(data)?;
    Ok(pt)
  }
}
