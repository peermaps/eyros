use tree::{Row,Coord,Point};
use std::marker::PhantomData;
use std::fmt::Debug;

#[derive(Debug)]
pub struct Sorted<A,B,C,D,E,F,V> {
  s0: Option<Vec<usize>>,
  s1: Option<Vec<usize>>,
  s2: Option<Vec<usize>>,
  s3: Option<Vec<usize>>,
  s4: Option<Vec<usize>>,
  s5: Option<Vec<usize>>,
  _marker0: PhantomData<A>,
  _marker1: PhantomData<B>,
  _marker2: PhantomData<C>,
  _marker3: PhantomData<D>,
  _marker4: PhantomData<E>,
  _marker5: PhantomData<F>,
  _marker6: PhantomData<V>
}

impl<A,B,C,D,E,F,V> Sorted<A,B,C,D,E,F,V> where
A: Debug+PartialOrd,
B: Debug+PartialOrd,
C: Debug+PartialOrd,
D: Debug+PartialOrd,
E: Debug+PartialOrd,
F: Debug+PartialOrd {
  fn new (rows: &Vec<Row<A,B,C,D,E,F,V>>) -> Self {
    let row0 = &rows[0];
    match row0 {
      Row::Insert(ref point) => Self::from_point(&point, rows),
      Row::Delete(ref point) => Self::from_point(&point, rows)
    }
  }
  #[inline]
  fn from_point (point: &Point<A,B,C,D,E,F,V>, rows: &Vec<Row<A,B,C,D,E,F,V>>) -> Self {
    let mut xs: Vec<usize> = (0..rows.len()).collect();
    xs.sort_unstable_by(|a,b| {
      Self::cmp(&Self::get0(&rows[*a]), &Self::get0(&rows[*b]))
    });
    let s0 = Some(xs);
    let mut s1 = None;
    let mut s2 = None;
    let mut s3 = None;
    let mut s4 = None;
    let mut s5 = None;
    match point {
      Point::P1(_,_) => {},
      _ => {
        let mut xs: Vec<usize> = (0..rows.len()).collect();
        xs.sort_unstable_by(|a,b| {
          Self::cmp(Self::get1(&rows[*a]), Self::get1(&rows[*b]))
        });
        s1 = Some(xs);
        match point {
          Point::P2(_,_,_) => {},
          _ => {
            let mut xs: Vec<usize> = (0..rows.len()).collect();
            xs.sort_unstable_by(|a,b| {
              Self::cmp(Self::get2(&rows[*a]), Self::get2(&rows[*b]))
            });
            s2 = Some(xs);
            match point {
              Point::P3(_,_,_,_) => {},
              _ => {
                let mut xs: Vec<usize> = (0..rows.len()).collect();
                xs.sort_unstable_by(|a,b| {
                  Self::cmp(Self::get3(&rows[*a]), Self::get3(&rows[*b]))
                });
                s3 = Some(xs);
                match point {
                  Point::P4(_,_,_,_,_) => {},
                  _ => {
                    let mut xs: Vec<usize> = (0..rows.len()).collect();
                    xs.sort_unstable_by(|a,b| {
                      Self::cmp(Self::get4(&rows[*a]), Self::get4(&rows[*b]))
                    });
                    s4 = Some(xs);
                    match point {
                      Point::P5(_,_,_,_,_,_) => {},
                      _ => {
                        let mut xs: Vec<usize> = (0..rows.len()).collect();
                        xs.sort_unstable_by(|a,b| {
                          Self::cmp(Self::get5(&rows[*a]), Self::get5(&rows[*b]))
                        });
                        s5 = Some(xs);
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    Self {
      s0, s1, s2, s3, s4, s5,
      _marker0: PhantomData,
      _marker1: PhantomData,
      _marker2: PhantomData,
      _marker3: PhantomData,
      _marker4: PhantomData,
      _marker5: PhantomData,
      _marker6: PhantomData
    }
  }
  fn get0 (row: &Row<A,B,C,D,E,F,V>) -> &Coord<A> {
    match *row {
      Row::Insert(ref point) => Self::get0p(point),
      Row::Delete(ref point) => Self::get0p(point)
    }
  }
  fn get0p (point: &Point<A,B,C,D,E,F,V>) -> &Coord<A> {
    match *point {
      Point::P1(ref p,_) => p,
      Point::P2(ref p,_,_) => p,
      Point::P3(ref p,_,_,_) => p,
      Point::P4(ref p,_,_,_,_) => p,
      Point::P5(ref p,_,_,_,_,_) => p,
      Point::P6(ref p,_,_,_,_,_,_) => p
    }
  }
  fn get1 (row: &Row<A,B,C,D,E,F,V>) -> &Coord<B> {
    match *row {
      Row::Insert(ref point) => Self::get1p(point),
      Row::Delete(ref point) => Self::get1p(point)
    }
  }
  fn get1p (point: &Point<A,B,C,D,E,F,V>) -> &Coord<B> {
    match *point {
      Point::P1(_,_) => panic!("no such field"),
      Point::P2(_,ref p,_) => p,
      Point::P3(_,ref p,_,_) => p,
      Point::P4(_,ref p,_,_,_) => p,
      Point::P5(_,ref p,_,_,_,_) => p,
      Point::P6(_,ref p,_,_,_,_,_) => p
    }
  }
  fn get2 (row: &Row<A,B,C,D,E,F,V>) -> &Coord<C> {
    match *row {
      Row::Insert(ref point) => Self::get2p(point),
      Row::Delete(ref point) => Self::get2p(point)
    }
  }
  fn get2p (point: &Point<A,B,C,D,E,F,V>) -> &Coord<C> {
    match point {
      Point::P1(_,_) => panic!("no such field"),
      Point::P2(_,_,_) => panic!("no such field"),
      Point::P3(_,_,ref p,_) => p,
      Point::P4(_,_,ref p,_,_) => p,
      Point::P5(_,_,ref p,_,_,_) => p,
      Point::P6(_,_,ref p,_,_,_,_) => p
    }
  }
  fn get3 (row: &Row<A,B,C,D,E,F,V>) -> &Coord<D> {
    match *row {
      Row::Insert(ref point) => Self::get3p(point),
      Row::Delete(ref point) => Self::get3p(point)
    }
  }
  fn get3p (point: &Point<A,B,C,D,E,F,V>) -> &Coord<D> {
    match point {
      Point::P1(_,_) => panic!("no such field"),
      Point::P2(_,_,_) => panic!("no such field"),
      Point::P3(_,_,_,_) => panic!("no such field"),
      Point::P4(_,_,_,ref p,_) => p,
      Point::P5(_,_,_,ref p,_,_) => p,
      Point::P6(_,_,_,ref p,_,_,_) => p
    }
  }
  fn get4 (row: &Row<A,B,C,D,E,F,V>) -> &Coord<E> {
    match *row {
      Row::Insert(ref point) => Self::get4p(point),
      Row::Delete(ref point) => Self::get4p(point)
    }
  }
  fn get4p (point: &Point<A,B,C,D,E,F,V>) -> &Coord<E> {
    match point {
      Point::P1(_,_) => panic!("no such field"),
      Point::P2(_,_,_) => panic!("no such field"),
      Point::P3(_,_,_,_) => panic!("no such field"),
      Point::P4(_,_,_,_,_) => panic!("no such field"),
      Point::P5(_,_,_,_,ref p,_) => p,
      Point::P6(_,_,_,_,ref p,_,_) => p
    }
  }
  fn get5 (row: &Row<A,B,C,D,E,F,V>) -> &Coord<F> {
    match *row {
      Row::Insert(ref point) => Self::get5p(point),
      Row::Delete(ref point) => Self::get5p(point)
    }
  }
  fn get5p (point: &Point<A,B,C,D,E,F,V>) -> &Coord<F> {
    match point {
      Point::P1(_,_) => panic!("no such field"),
      Point::P2(_,_,_) => panic!("no such field"),
      Point::P3(_,_,_,_) => panic!("no such field"),
      Point::P4(_,_,_,_,_) => panic!("no such field"),
      Point::P5(_,_,_,_,_,_) => panic!("no such field"),
      Point::P6(_,_,_,_,_,ref p,_) => p
    }
  }
  fn cmp<T> (a: &Coord<T>, b: &Coord<T>) -> std::cmp::Ordering
  where T: PartialOrd {
    match (a,b) {
      (Coord::Point(ref pa),Coord::Point(ref pb)) => {
        match pa.partial_cmp(pb) {
          Some(x) => x,
          None => std::cmp::Ordering::Less
        }
      },
      (Coord::Range(_,ref pa),Coord::Range(_,ref pb)) => {
        match pa.partial_cmp(pb) {
          Some(x) => x,
          None => std::cmp::Ordering::Less
        }
      },
      _ => panic!("point/range mismatch")
    }
  }
}
