use std::cmp::Ordering;
use std::ops::{Div,Add};
use std::mem::transmute;
use failure::Error;
use bincode::{serialize,deserialize};
use serde::{Serialize,de::DeserializeOwned};

pub trait Point: Copy {
  fn cmp_at (&self, &Self, usize) -> Ordering where Self: Sized;
  fn midpoint_upper (&self, &Self) -> Self where Self: Sized;
  fn serialize_at (&self, usize) -> Result<Vec<u8>,Error>;
}

pub trait Num<T>: PartialOrd+Copy+Serialize+DeserializeOwned
+Scalar+From<u8>+Div<T,Output=T>+Add<T,Output=T> {}
impl<T> Num<T> for T where T: PartialOrd+Copy+Serialize+DeserializeOwned
+Scalar+From<u8>+Div<T,Output=T>+Add<T,Output=T> {}

pub trait Scalar: Copy+Sized+'static {}
impl Scalar for f32 {}
impl Scalar for f64 {}
impl Scalar for u8 {}
impl Scalar for u16 {}
impl Scalar for u32 {}
impl Scalar for u64 {}
impl Scalar for i8 {}
impl Scalar for i16 {}
impl Scalar for i32 {}
impl Scalar for i64 {}

impl<A,B> Point for (A,B) where A: Num<A>, B: Num<B> {
  fn cmp_at (&self, other: &Self, level: usize) -> Ordering {
    let order = match level%2 {
      0 => if self.0 == other.0
        { Some(Ordering::Equal) } else { self.0.partial_cmp(&other.0) }
      _ => if self.1 == other.1
        { Some(Ordering::Equal) } else { self.1.partial_cmp(&other.1) }
    };
    match order { Some(x) => x, None => Ordering::Less }
  }
  fn midpoint_upper (&self, other: &Self) -> Self {
    let a = (self.0 + other.0) / 2.into();
    let b = (self.1 + other.1) / 2.into();
    (a,b)
  }
  fn serialize_at (&self, level: usize) -> Result<Vec<u8>,Error> {
    let buf: Vec<u8> = match level%2 {
      0 => serialize(&self.0)?,
      _ => serialize(&self.1)?
    };
    Ok(buf)
  }
}

impl<A,B> Point for (A,(B,B)) where A: Num<A>, B: Num<B> {
  fn cmp_at (&self, other: &Self, level: usize) -> Ordering {
    let order = match level%2 {
      0 => if self.0 == other.0
        { Some(Ordering::Equal) } else { self.0.partial_cmp(&other.0) }
      _ => if (self.1).0 <= (other.1).1 && (other.1).0 <= (self.1).1
        { Some(Ordering::Equal) } else { (self.1).0.partial_cmp(&(other.1).0) }
    };
    match order { Some(x) => x, None => Ordering::Less }
  }
  fn midpoint_upper (&self, other: &Self) -> Self {
    let a = (self.0 + other.0) / 2.into();
    let b = ((self.1).1 + (other.1).1) / 2.into();
    (a,(b,b))
  }
  fn serialize_at (&self, level: usize) -> Result<Vec<u8>,Error> {
    let buf: Vec<u8> = match level%2 {
      0 => serialize(&self.0)?,
      _ => serialize(&self.1)?
    };
    Ok(buf)
  }
}

impl<A,B> Point for ((A,A),B) where A: Num<A>, B: Num<B> {
  fn cmp_at (&self, other: &Self, level: usize) -> Ordering {
    let order = match level%2 {
      0 => if (self.0).0 <= (other.0).1 && (other.0).0 <= (self.0).1
        { Some(Ordering::Equal) } else { (self.0).0.partial_cmp(&(other.0).0) }
      _ => if self.1 == other.1
        { Some(Ordering::Equal) } else { self.1.partial_cmp(&other.1) }
    };
    match order { Some(x) => x, None => Ordering::Less }
  }
  fn midpoint_upper (&self, other: &Self) -> Self {
    let a = ((self.0).1 + (other.0).1) / 2.into();
    let b = (self.1 + other.1) / 2.into();
    ((a,a),b)
  }
  fn serialize_at (&self, level: usize) -> Result<Vec<u8>,Error> {
    let buf: Vec<u8> = match level%2 {
      0 => serialize(&self.0)?,
      _ => serialize(&self.1)?
    };
    Ok(buf)
  }
}

impl<A,B> Point for ((A,A),(B,B)) where A: Num<A>, B: Num<B> {
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
  fn serialize_at (&self, level: usize) -> Result<Vec<u8>,Error> {
    let buf: Vec<u8> = match level%2 {
      0 => serialize(&self.0)?,
      _ => serialize(&self.1)?
    };
    Ok(buf)
  }
}

impl<A,B,C> Point for (A,B,C) where A: Num<A>, B: Num<B>, C: Num<C> {
  fn cmp_at (&self, other: &Self, level: usize) -> Ordering {
    let order = match level%2 {
      0 => if self.0 == other.0
        { Some(Ordering::Equal) } else { self.0.partial_cmp(&other.0) }
      1 => if self.1 == other.1
        { Some(Ordering::Equal) } else { self.1.partial_cmp(&other.1) }
      _ => if self.2 == other.2
        { Some(Ordering::Equal) } else { self.2.partial_cmp(&other.2) }
    };
    match order { Some(x) => x, None => Ordering::Less }
  }
  fn midpoint_upper (&self, other: &Self) -> Self {
    let a = (self.0 + other.0) / 2.into();
    let b = (self.1 + other.1) / 2.into();
    let c = (self.2 + other.2) / 2.into();
    (a,b,c)
  }
  fn serialize_at (&self, level: usize) -> Result<Vec<u8>,Error> {
    let buf: Vec<u8> = match level%2 {
      0 => serialize(&self.0)?,
      1 => serialize(&self.1)?,
      _ => serialize(&self.2)?
    };
    Ok(buf)
  }
}

impl<A,B,C> Point for (A,(B,B),C) where A: Num<A>, B: Num<B>, C: Num<C> {
  fn cmp_at (&self, other: &Self, level: usize) -> Ordering {
    let order = match level%2 {
      0 => if self.0 == other.0
        { Some(Ordering::Equal) } else { self.0.partial_cmp(&other.0) },
      1 => if (self.1).0 <= (other.1).1 && (other.1).0 <= (self.1).1
        { Some(Ordering::Equal) } else { (self.1).0.partial_cmp(&(other.1).0) },
      _ => if self.2 == other.2
        { Some(Ordering::Equal) } else { self.2.partial_cmp(&other.2) }
    };
    match order { Some(x) => x, None => Ordering::Less }
  }
  fn midpoint_upper (&self, other: &Self) -> Self {
    let a = (self.0 + other.0) / 2.into();
    let b = ((self.1).1 + (other.1).1) / 2.into();
    let c = (self.2 + other.2) / 2.into();
    (a,(b,b),c)
  }
  fn serialize_at (&self, level: usize) -> Result<Vec<u8>,Error> {
    let buf: Vec<u8> = match level%2 {
      0 => serialize(&self.0)?,
      1 => serialize(&self.1)?,
      _ => serialize(&self.2)?
    };
    Ok(buf)
  }
}

impl<A,B,C> Point for ((A,A),B,C) where A: Num<A>, B: Num<B>, C: Num<C> {
  fn cmp_at (&self, other: &Self, level: usize) -> Ordering {
    let order = match level%2 {
      0 => if (self.0).0 <= (other.0).1 && (other.0).0 <= (self.0).1
        { Some(Ordering::Equal) } else { (self.0).0.partial_cmp(&(other.0).0) },
      1 => if self.1 == other.1
        { Some(Ordering::Equal) } else { self.1.partial_cmp(&other.1) },
      _ => if self.2 == other.2
        { Some(Ordering::Equal) } else { self.2.partial_cmp(&other.2) }
    };
    match order { Some(x) => x, None => Ordering::Less }
  }
  fn midpoint_upper (&self, other: &Self) -> Self {
    let a = ((self.0).1 + (other.0).1) / 2.into();
    let b = (self.1 + other.1) / 2.into();
    let c = (self.2 + other.2) / 2.into();
    ((a,a),b,c)
  }
  fn serialize_at (&self, level: usize) -> Result<Vec<u8>,Error> {
    let buf: Vec<u8> = match level%2 {
      0 => serialize(&self.0)?,
      1 => serialize(&self.1)?,
      _ => serialize(&self.2)?
    };
    Ok(buf)
  }
}

impl<A,B,C> Point for ((A,A),(B,B),C)
where A: Num<A>, B: Num<B>, C: Num<C> {
  fn cmp_at (&self, other: &Self, level: usize) -> Ordering {
    let order = match level%3 {
      0 => if (self.0).0 <= (other.0).1 && (other.0).0 <= (self.0).1
        { Some(Ordering::Equal) } else { (self.0).0.partial_cmp(&(other.0).0) },
      1 => if (self.1).0 <= (other.1).1 && (other.1).0 <= (self.1).1
        { Some(Ordering::Equal) } else { (self.1).0.partial_cmp(&(other.1).0) },
      _ => if self.2 == other.2
        { Some(Ordering::Equal) } else { self.2.partial_cmp(&other.2) }
    };
    match order { Some(x) => x, None => Ordering::Less }
  }
  fn midpoint_upper (&self, other: &Self) -> Self {
    let a = ((self.0).1 + (other.0).1) / 2.into();
    let b = ((self.1).1 + (other.1).1) / 2.into();
    let c = (self.2 + other.2) / 2.into();
    ((a,a),(b,b),c)
  }
  fn serialize_at (&self, level: usize) -> Result<Vec<u8>,Error> {
    let buf: Vec<u8> = match level%3 {
      0 => serialize(&self.0)?,
      1 => serialize(&self.1)?,
      _ => serialize(&self.2)?
    };
    Ok(buf)
  }
}

impl<A,B,C> Point for (A,B,(C,C)) where A: Num<A>, B: Num<B>, C: Num<C> {
  fn cmp_at (&self, other: &Self, level: usize) -> Ordering {
    let order = match level%2 {
      0 => if self.0 == other.0
        { Some(Ordering::Equal) } else { self.0.partial_cmp(&other.0) },
      1 => if self.1 == other.1
        { Some(Ordering::Equal) } else { self.1.partial_cmp(&other.1) },
      _ => if (self.2).0 <= (other.2).1 && (other.2).0 <= (self.2).1
        { Some(Ordering::Equal) } else { (self.2).0.partial_cmp(&(other.2).0) }
    };
    match order { Some(x) => x, None => Ordering::Less }
  }
  fn midpoint_upper (&self, other: &Self) -> Self {
    let a = (self.0 + other.0) / 2.into();
    let b = (self.1 + other.1) / 2.into();
    let c = ((self.2).1 + (other.2).1) / 2.into();
    (a,b,(c,c))
  }
  fn serialize_at (&self, level: usize) -> Result<Vec<u8>,Error> {
    let buf: Vec<u8> = match level%2 {
      0 => serialize(&self.0)?,
      1 => serialize(&self.1)?,
      _ => serialize(&self.2)?
    };
    Ok(buf)
  }
}

impl<A,B,C> Point for (A,(B,B),(C,C)) where A: Num<A>, B: Num<B>, C: Num<C> {
  fn cmp_at (&self, other: &Self, level: usize) -> Ordering {
    let order = match level%2 {
      0 => if self.0 == other.0
        { Some(Ordering::Equal) } else { self.0.partial_cmp(&other.0) },
      1 => if (self.1).0 <= (other.1).1 && (other.1).0 <= (self.1).1
        { Some(Ordering::Equal) } else { (self.1).0.partial_cmp(&(other.1).0) },
      _ => if (self.2).0 <= (other.2).1 && (other.2).0 <= (self.2).1
        { Some(Ordering::Equal) } else { (self.2).0.partial_cmp(&(other.2).0) }
    };
    match order { Some(x) => x, None => Ordering::Less }
  }
  fn midpoint_upper (&self, other: &Self) -> Self {
    let a = (self.0 + other.0) / 2.into();
    let b = ((self.1).1 + (other.1).1) / 2.into();
    let c = ((self.2).1 + (other.2).1) / 2.into();
    (a,(b,b),(c,c))
  }
  fn serialize_at (&self, level: usize) -> Result<Vec<u8>,Error> {
    let buf: Vec<u8> = match level%2 {
      0 => serialize(&self.0)?,
      1 => serialize(&self.1)?,
      _ => serialize(&self.2)?
    };
    Ok(buf)
  }
}

impl<A,B,C> Point for ((A,A),B,(C,C)) where A: Num<A>, B: Num<B>, C: Num<C> {
  fn cmp_at (&self, other: &Self, level: usize) -> Ordering {
    let order = match level%2 {
      0 => if (self.0).0 <= (other.0).1 && (other.0).0 <= (self.0).1
        { Some(Ordering::Equal) } else { (self.0).0.partial_cmp(&(other.0).0) }
      1 => if self.1 == other.1
        { Some(Ordering::Equal) } else { self.1.partial_cmp(&other.1) },
      _ => if (self.2).0 <= (other.2).1 && (other.2).0 <= (self.2).1
        { Some(Ordering::Equal) } else { (self.2).0.partial_cmp(&(other.2).0) }
    };
    match order { Some(x) => x, None => Ordering::Less }
  }
  fn midpoint_upper (&self, other: &Self) -> Self {
    let a = ((self.0).1 + (other.0).1) / 2.into();
    let b = (self.1 + other.1) / 2.into();
    let c = ((self.2).1 + (other.2).1) / 2.into();
    ((a,a),b,(c,c))
  }
  fn serialize_at (&self, level: usize) -> Result<Vec<u8>,Error> {
    let buf: Vec<u8> = match level%2 {
      0 => serialize(&self.0)?,
      1 => serialize(&self.1)?,
      _ => serialize(&self.2)?
    };
    Ok(buf)
  }
}

impl<A,B,C> Point for ((A,A),(B,B),(C,C))
where A: Num<A>, B: Num<B>, C: Num<C> {
  fn cmp_at (&self, other: &Self, level: usize) -> Ordering {
    let order = match level%3 {
      0 => if (self.0).0 <= (other.0).1 && (other.0).0 <= (self.0).1
        { Some(Ordering::Equal) } else { (self.0).0.partial_cmp(&(other.0).0) },
      1 => if (self.1).0 <= (other.1).1 && (other.1).0 <= (self.1).1
        { Some(Ordering::Equal) } else { (self.1).0.partial_cmp(&(other.1).0) },
      _ => if (self.2).0 <= (other.2).1 && (other.2).0 <= (self.2).1
        { Some(Ordering::Equal) } else { (self.2).0.partial_cmp(&(other.2).0) }
    };
    match order { Some(x) => x, None => Ordering::Less }
  }
  fn midpoint_upper (&self, other: &Self) -> Self {
    let a = ((self.0).1 + (other.0).1) / 2.into();
    let b = ((self.1).1 + (other.1).1) / 2.into();
    let c = ((self.2).1 + (other.2).1) / 2.into();
    ((a,a),(b,b),(c,c))
  }
  fn serialize_at (&self, level: usize) -> Result<Vec<u8>,Error> {
    let buf: Vec<u8> = match level%3 {
      0 => serialize(&self.0)?,
      1 => serialize(&self.1)?,
      _ => serialize(&self.2)?
    };
    Ok(buf)
  }
}
