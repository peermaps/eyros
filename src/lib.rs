extern crate random_access_storage;
extern crate failure;

mod db3;
pub use db3::{DB3,Row3};

mod meta;

#[derive(Debug)]
pub enum Coord<T> {
  Point(T),
  Range(T,T)
}
