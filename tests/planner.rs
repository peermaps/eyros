#[path="../src/planner.rs"]
mod planner;
use planner::plan;

#[path="../src/bits.rs"]
mod bits;
use bits::{num_to_bits};

macro_rules! bits {
  (_ 0) => { false };
  (_ 1) => { true };
  ($($x:tt),*) => { vec![$(bits![_ $x]),*] };
}

#[test]
fn planner () {
  let p0 = plan(
    &num_to_bits(6),
    &bits![0,1,1,0,1]
  );
  assert_eq!(p0, vec![(3,vec![1,2],vec![1])]);
  check_sums(p0);

  let p1 = plan(
    &bits![0,1,1],
    &bits![0,1,1,0,1]
  );
  assert_eq!(p1, vec![(3,vec![1,2],vec![1])]);
  check_sums(p1);

  let p2 = plan(
    &bits![1,1,1,0,1],
    &bits![0,1,1,0,1,1,1,0]
  );
  assert_eq!(p2, vec![
    (0,vec![0],vec![]),
    (3,vec![1,2],vec![1]),
    (7,vec![4],vec![4,5,6])
  ]);
  check_sums(p2);

  let p3 = plan(
    &bits![0,1,1,0,0,0,1,0,1,1,1,0,1,0,0],
    &bits![1,0,1,1,1,0,1,0,1,0,0,1,1,1,0]
  );
  assert_eq!(p3, vec![
    (1,vec![1],vec![]),
    (5,vec![2],vec![2,3,4]),
    (7,vec![6],vec![6]),
    (14,vec![8,9,10,12],vec![8,11,13])
  ]);
  check_sums(p3);

  let p4 = plan(
    &bits![1,0,1,1],
    &bits![1,1,0,0]
  );
  assert_eq!(p4, vec![
    (4,vec![0,2,3],vec![0,1])
  ]);
  check_sums(p4);

  let p5 = plan(
    &bits![1,1,1,1,1,1],
    &bits![1,1,1,1,1,1]
  );
  assert_eq!(p5, vec![
    (6,vec![0,1,2,3,4,5],vec![0])
  ]);
  check_sums(p5);
}

#[test]
fn planner_empty_tree () {
  let p0 = plan(
    &bits![1],
    &bits![]
  );
  assert_eq!(p0, vec![(0,vec![0],vec![])]);

  let p1 = plan(
    &bits![1,0,1],
    &bits![]
  );
  assert_eq!(p1, vec![
    (0,vec![0],vec![]),
    (2,vec![2],vec![])
  ]);

  let p2 = plan(
    &bits![0,0,0,1],
    &bits![]
  );
  assert_eq!(p2, vec![
    (3,vec![3],vec![])
  ]);

  let p3 = plan(
    &bits![],
    &bits![]
  );
  assert_eq!(p3, vec![]);
}

fn check_sums (merges: Vec<(usize,Vec<usize>,Vec<usize>)>) -> () {
  for m in merges {
    let mut sum = 0;
    for x in m.1 { sum += 2u64.pow(x as u32) }
    for x in m.2 { sum += 2u64.pow(x as u32) }
    assert_eq!(2u64.pow(m.0 as u32), sum);
  }
}
