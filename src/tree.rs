use desert::ToBytes;
use crate::{Scalar,Point,Value,Coord};

#[derive(Debug)]
pub enum Node2<X,Y,V> where X: Scalar, Y: Scalar, V: Value {
  Branch(Branch2<X,Y,V>),
  Data(Vec<((Coord<X>,Coord<Y>),V)>),
  //Ref(u64)
}

#[derive(Debug)]
pub struct Branch2<X,Y,V> where X: Scalar, Y: Scalar, V: Value {
  pub pivots: (Option<Vec<X>>,Option<Vec<Y>>),
  pub intersections: Vec<Node2<X,Y,V>>,
  pub nodes: Vec<Node2<X,Y,V>>,
}

impl<X,Y,V> Branch2<X,Y,V> where X: Scalar, Y: Scalar, V: Value {
  fn dim() -> usize { 2 }
  pub fn build(branch_factor: usize, inserts: &[(&(Coord<X>,Coord<Y>),&V)]) -> Node2<X,Y,V> {
    let sorted = (
      {
        let mut xs: Vec<usize> = (0..inserts.len()).collect();
        xs.sort_unstable_by(|a,b| {
          coord_cmp(&(inserts[*a].0).0,&(inserts[*b].0).0).unwrap()
        });
        xs
      },
      {
        let mut xs: Vec<usize> = (0..inserts.len()).collect();
        xs.sort_unstable_by(|a,b| {
          coord_cmp(&(inserts[*a].0).1,&(inserts[*b].0).1).unwrap()
        });
        xs
      },
    );
    Self::from_sorted(
      branch_factor, 0, inserts,
      (sorted.0.as_slice(), sorted.1.as_slice()),
      &mut vec![false;inserts.len()]
    )
  }
  fn from_sorted(branch_factor: usize, level: usize, inserts: &[(&(Coord<X>,Coord<Y>),&V)],
  sorted: (&[usize],&[usize]), matched: &mut [bool]) -> Node2<X,Y,V> {
    if sorted.0.len() == 0 {
      return Node2::Data(vec![]);
    } else if sorted.0.len() < branch_factor {
      return Node2::Data(sorted.0.iter().map(|i| {
        let pv = &inserts[*i];
        (((pv.0).0.clone(),(pv.0).1.clone()),pv.1.clone())
      }).collect());
    }
    let n = (branch_factor-1).min(sorted.0.len()-1); // number of pivots
    let is_min = (level / Self::dim()) % 2 != 0;
    let mut pivots = (None,None);
    match level % Self::dim() {
      0 => {
        let mut ps = match sorted.0.len() {
          0 => panic!["not enough data to create a branch"],
          1 => match &(inserts[sorted.0[0]].0).0 {
            Coord::Scalar(x) => {
              vec![find_separation(x,x,x,x,is_min)]
            },
            Coord::Interval(min,max) => {
              vec![find_separation(min,max,min,max,is_min)]
            }
          },
          2 => {
            let a = match &(inserts[sorted.0[0]].0).0 {
              Coord::Scalar(x) => (x,x),
              Coord::Interval(min,max) => (min,max),
            };
            let b = match &(inserts[sorted.0[1]].0).0 {
              Coord::Scalar(x) => (x,x),
              Coord::Interval(min,max) => (min,max),
            };
            vec![find_separation(a.0,a.1,b.0,b.1,is_min)]
          },
          _ => {
            (0..n).map(|k| {
              let m = k * sorted.0.len() / (n+1);
              let a = match &(inserts[sorted.0[m+0]].0).0 {
                Coord::Scalar(x) => (x,x),
                Coord::Interval(min,max) => (min,max),
              };
              let b = match &(inserts[sorted.0[m+1]].0).0 {
                Coord::Scalar(x) => (x,x),
                Coord::Interval(min,max) => (min,max),
              };
              find_separation(a.0,a.1,b.0,b.1,is_min)
            }).collect()
          }
        };
        ps.sort_unstable_by(|a,b| {
          a.partial_cmp(b).unwrap()
        });
        pivots.0 = Some(ps);
      },
      1 => {
        let mut ps = match sorted.1.len() {
          0 => panic!["not enough data to create a branch"],
          1 => match &(inserts[sorted.1[0]].0).1 {
            Coord::Scalar(x) => {
              vec![find_separation(x,x,x,x,is_min)]
            },
            Coord::Interval(min,max) => {
              vec![find_separation(min,max,min,max,is_min)]
            }
          },
          2 => {
            let a = match &(inserts[sorted.1[0]].0).1 {
              Coord::Scalar(x) => (x,x),
              Coord::Interval(min,max) => (min,max),
            };
            let b = match &(inserts[sorted.1[1]].0).1 {
              Coord::Scalar(x) => (x,x),
              Coord::Interval(min,max) => (min,max),
            };
            vec![find_separation(a.0,a.1,b.0,b.1,is_min)]
          },
          _ => {
            (0..n).map(|k| {
              let m = k * sorted.1.len() / (n+1);
              let a = match &(inserts[sorted.1[m+0]].0).1 {
                Coord::Scalar(x) => (x,x),
                Coord::Interval(min,max) => (min,max),
              };
              let b = match &(inserts[sorted.1[m+1]].0).1 {
                Coord::Scalar(x) => (x,x),
                Coord::Interval(min,max) => (min,max),
              };
              find_separation(a.0,a.1,b.0,b.1,is_min)
            }).collect()
          }
        };
        ps.sort_unstable_by(|a,b| {
          a.partial_cmp(b).unwrap()
        });
        pivots.1 = Some(ps);
      },
      _ => panic!["unexpected level modulo dimension"]
    };
    //pad_pivots(n, &mut pivots);
    //eprintln!["n={}, pivots={:?}", n, pivots];

    let intersections: Vec<Node2<X,Y,V>> = match level % Self::dim() {
      0 => pivots.0.as_ref().unwrap().iter().map(|pivot| {
        let indexes: Vec<usize> = sorted.0.iter()
          .map(|j| *j)
          .filter(|j| {
            !matched[*j] && intersect_pivot(&(inserts[*j].0).0, pivot)
          })
          .collect();
        if indexes.len() == sorted.0.len() {
          //eprintln!["{} == {}", indexes.len(), sorted.0.len()];
          return Node2::Data(indexes.iter().map(|i| {
            let pv = &inserts[*i];
            matched[*i] = true;
            (((pv.0).0.clone(),(pv.0).1.clone()),pv.1.clone())
          }).collect());
        }
        let b = Branch2::from_sorted(
          branch_factor,
          level+1,
          inserts,
          (
            sorted.1.iter()
              .map(|j| *j)
              .filter(|j| {
                !matched[*j] && intersect_pivot(&(inserts[*j].0).0, pivot)
              })
              .collect::<Vec<usize>>().as_slice(),
            &indexes
          ),
          matched
        );
        indexes.iter().for_each(|i| {
          matched[*i] = true;
        });
        b
      }).collect(),
      1 => pivots.1.as_ref().unwrap().iter().map(|pivot| {
        let indexes: Vec<usize> = sorted.1.iter()
          .map(|j| *j)
          .filter(|j| {
            !matched[*j] && intersect_pivot(&(inserts[*j].0).1, pivot)
          })
          .collect();
        if indexes.len() == sorted.1.len() {
          //eprintln!["{} == {}", indexes.len(), sorted.0.len()];
          return Node2::Data(indexes.iter().map(|i| {
            let pv = &inserts[*i];
            matched[*i] = true;
            (((pv.0).0.clone(),(pv.0).1.clone()),pv.1.clone())
          }).collect());
        }
        let b = Branch2::from_sorted(
          branch_factor,
          level+1,
          inserts,
          (
            sorted.1.iter()
              .map(|j| *j)
              .filter(|j| {
                !matched[*j] && intersect_pivot(&(inserts[*j].0).1, pivot)
              })
              .collect::<Vec<usize>>().as_slice(),
            &indexes
          ),
          matched
        );
        indexes.iter().for_each(|i| {
          matched[*i] = true;
        });
        b
      }).collect(),
      _ => panic!["unexpected level modulo dimension"]
    };

    let nodes = match level % Self::dim() {
      0 => {
        let mut nodes: Vec<Node2<X,Y,V>> = pivots.0.as_ref().unwrap().iter().map(|pivot| {
          let next_sorted: (Vec<usize>,Vec<usize>) = (
            sorted.0.iter().map(|j| *j).filter(|j| {
              !matched[*j] && coord_cmp_pivot(&(inserts[*j].0).0, pivot)
                == Some(std::cmp::Ordering::Less)
            }).collect(),
            sorted.1.iter().map(|j| *j).filter(|j| {
              !matched[*j] && coord_cmp_pivot(&(inserts[*j].0).0, pivot)
                == Some(std::cmp::Ordering::Less)
            }).collect()
          );
          for j in next_sorted.0.iter() {
            matched[*j] = true;
          }
          Branch2::from_sorted(
            branch_factor,
            level+1,
            inserts,
            (next_sorted.0.as_slice(), next_sorted.1.as_slice()),
            matched
          )
        }).collect();
        nodes.push({
          let next_sorted: (Vec<usize>,Vec<usize>) = (
            sorted.0.iter().map(|j| *j).filter(|j| !matched[*j]).collect(),
            sorted.1.iter().map(|j| *j).filter(|j| !matched[*j]).collect()
          );
          Branch2::from_sorted(
            branch_factor,
            level+1,
            inserts,
            (next_sorted.0.as_slice(), next_sorted.1.as_slice()),
            matched
          )
        });
        nodes
      },
      1 => {
        let mut nodes: Vec<Node2<X,Y,V>> = pivots.1.as_ref().unwrap().iter().map(|pivot| {
          let next_sorted: (Vec<usize>,Vec<usize>) = (
            sorted.0.iter().map(|j| *j).filter(|j| {
              !matched[*j] && coord_cmp_pivot(&(inserts[*j].0).1, pivot)
                == Some(std::cmp::Ordering::Less)
            }).collect(),
            sorted.1.iter().map(|j| *j).filter(|j| {
              !matched[*j] && coord_cmp_pivot(&(inserts[*j].0).1, pivot)
                == Some(std::cmp::Ordering::Less)
            }).collect()
          );
          for j in next_sorted.1.iter() {
            matched[*j] = true;
          }
          Branch2::from_sorted(
            branch_factor,
            level+1,
            inserts,
            (next_sorted.0.as_slice(), next_sorted.1.as_slice()),
            matched
          )
        }).collect();
        nodes.push({
          let next_sorted: (Vec<usize>,Vec<usize>) = (
            sorted.0.iter().map(|j| *j).filter(|j| !matched[*j]).collect(),
            sorted.1.iter().map(|j| *j).filter(|j| !matched[*j]).collect()
          );
          Branch2::from_sorted(
            branch_factor,
            level+1,
            inserts,
            (next_sorted.0.as_slice(), next_sorted.1.as_slice()),
            matched
          )
        });
        nodes
      },
      _ => panic!["unexpected level modulo dimension"]
    };

    let node_count = nodes.iter().fold(0usize, |count,node| {
      count + match node {
        Node2::Data(bs) => if bs.is_empty() { 0 } else { 1 },
        Node2::Branch(_) => 1,
      }
    });
    if node_count <= 1 {
      return Node2::Data(inserts.iter().map(|((x,y),v)| {
        ((x.clone(),y.clone()),(*v).clone())
      }).collect());
    }

    Node2::Branch(Self {
      pivots,
      intersections,
      nodes,
    })
  }
}

#[async_trait::async_trait]
pub trait Tree<P,V>: Send+Sync+ToBytes where P: Point, V: Value {
  fn build(branch_factor: usize, rows: &[(&P,&V)]) -> Self where Self: Sized;
  fn list(&mut self) -> Vec<(P,V)>;
  fn merge(branch_factor: usize, trees: &mut [&mut Self]) -> Self where Self: Sized;
}

#[derive(Debug)]
pub struct Tree2<X,Y,V> where X: Scalar, Y: Scalar, V: Value {
  pub root: Node2<X,Y,V>,
  pub bounds: (X,Y,X,Y),
  pub count: usize,
}

impl<X,Y,V> Tree<(Coord<X>,Coord<Y>),V> for Tree2<X,Y,V> where X: Scalar, Y: Scalar, V: Value {
  fn build(branch_factor: usize, rows: &[(&(Coord<X>,Coord<Y>),&V)]) -> Self {
    let ibounds = (
      match (rows[0].0).0.clone() {
        Coord::Scalar(x) => x,
        Coord::Interval(x,_) => x,
      },
      match (rows[0].0).1.clone() {
        Coord::Scalar(x) => x,
        Coord::Interval(x,_) => x,
      },
      match (rows[0].0).0.clone() {
        Coord::Scalar(x) => x,
        Coord::Interval(_,x) => x,
      },
      match (rows[0].0).1.clone() {
        Coord::Scalar(x) => x,
        Coord::Interval(_,x) => x,
      }
    );
    Self {
      root: Branch2::build(branch_factor, rows),
      count: rows.len(),
      bounds: rows[1..].iter().fold(ibounds, |bounds,row| {
        (
          coord_min_x(&(row.0).0, &bounds.0),
          coord_min_x(&(row.0).1, &bounds.1),
          coord_max_x(&(row.0).0, &bounds.2),
          coord_max_x(&(row.0).1, &bounds.3)
        )
      })
    }
  }
  fn list(&mut self) -> Vec<((Coord<X>,Coord<Y>),V)> {
    let mut cursors = vec![&self.root];
    let mut rows = vec![];
    while let Some(c) = cursors.pop() {
      match c {
        Node2::Branch(branch) => {
          for b in branch.intersections.iter() {
            cursors.push(b);
          }
          for b in branch.nodes.iter() {
            cursors.push(b);
          }
        },
        Node2::Data(data) => {
          rows.extend(data.iter().map(|pv| {
            (pv.0.clone(),pv.1.clone())
          }).collect::<Vec<_>>());
        }
      }
    }
    rows
  }
  fn merge(branch_factor: usize, trees: &mut [&mut Self]) -> Self {
    let mut rows = vec![];
    let mut lists = vec![];
    for tree in trees.iter_mut() {
      lists.push(tree.list());
    }
    for list in lists.iter_mut() {
      rows.extend(list.iter().map(|pv| {
        (&pv.0,&pv.1)
      }).collect::<Vec<_>>());
    }
    // todo: split large intersecting buckets
    Self::build(branch_factor, rows.as_slice())
  }
}

fn find_separation<X>(amin: &X, amax: &X, bmin: &X, bmax: &X, is_min: bool) -> X where X: Scalar {
  if is_min && intersect_iv(amin, amax, bmin, bmax) {
    ((*amin).clone() + (*bmin).clone()) / 2.into()
  } else if !is_min && intersect_iv(amin, amax, bmin, bmax) {
    ((*amax).clone() + (*bmax).clone()) / 2.into()
  } else {
    ((*amax).clone() + (*bmin).clone()) / 2.into()
  }
}

fn intersect_iv<X>(a0: &X, a1: &X, b0: &X, b1: &X) -> bool where X: PartialOrd {
  a0 <= b1 && a1 >= b0
}

fn intersect_pivot<X>(c: &Coord<X>, p: &X) -> bool where X: Scalar {
  match c {
    Coord::Scalar(x) => *x == *p,
    Coord::Interval(min,max) => *min <= *p && *p <= *max,
  }
}

fn coord_cmp<X>(x: &Coord<X>, y: &Coord<X>) -> Option<std::cmp::Ordering> where X: Scalar {
  match (x,y) {
    (Coord::Scalar(a),Coord::Scalar(b)) => a.partial_cmp(b),
    (Coord::Scalar(a),Coord::Interval(b,_)) => a.partial_cmp(b),
    (Coord::Interval(a,_),Coord::Scalar(b)) => a.partial_cmp(b),
    (Coord::Interval(a,_),Coord::Interval(b,_)) => a.partial_cmp(b),
  }
}

fn coord_min_x<X>(x: &Coord<X>, r: &X) -> X where X: Scalar {
  let l = match x {
    Coord::Scalar(a) => a,
    Coord::Interval(a,_) => a,
  };
  match l.partial_cmp(r) {
    None => l.clone(),
    Some(std::cmp::Ordering::Less) => l.clone(),
    Some(std::cmp::Ordering::Equal) => l.clone(),
    Some(std::cmp::Ordering::Greater) => r.clone(),
  }
}

fn coord_max_x<X>(x: &Coord<X>, r: &X) -> X where X: Scalar {
  let l = match x {
    Coord::Scalar(a) => a,
    Coord::Interval(a,_) => a,
  };
  match l.partial_cmp(r) {
    None => l.clone(),
    Some(std::cmp::Ordering::Less) => r.clone(),
    Some(std::cmp::Ordering::Equal) => r.clone(),
    Some(std::cmp::Ordering::Greater) => l.clone(),
  }
}

fn coord_cmp_pivot<X>(x: &Coord<X>, p: &X) -> Option<std::cmp::Ordering> where X: Scalar {
  match x {
    Coord::Scalar(a) => a.partial_cmp(p),
    Coord::Interval(a,_) => a.partial_cmp(p),
  }
}
