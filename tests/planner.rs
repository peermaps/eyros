extern crate eyros;
#[path="../src/planner.rs"]
mod planner;
use planner::Planner;

macro_rules! bits {
  (_ 0) => { false };
  (_ 1) => { true };
  ($($x:tt),*) => { vec![$(bits![_ $x]),*] };
}

#[test]
fn planner () {
  let p0 = Planner::new(6, bits![0,1,1,0,1]);
  assert_eq!(p0.inputs, bits![0,1]);
  assert_eq!(p0.outputs, bits![0,0,0,1]);

  let p1 = Planner::new(
    Planner::bits_to_num(bits![0,1,1]),
    bits![0,1,1,0,1]
  );
  assert_eq!(p1.inputs, bits![0,1]);
  assert_eq!(p1.outputs, bits![0,0,0,1]);

  let p2 = Planner::new(
    Planner::bits_to_num(bits![1,1,1,0,1]),
    bits![0,1,1,0,1,1,1,0]
  );
  assert_eq!(p2.inputs, bits![0,1,0,0,1,1,1]);
  assert_eq!(p2.outputs, bits![1,0,0,1,0,0,0,1]);
}

#[test]
fn planner_empty_tree () {
  let p0 = Planner::new(
    Planner::bits_to_num(bits![1]),
    bits![]
  );
  assert_eq!(p0.inputs, bits![]);
  assert_eq!(p0.outputs, bits![1]);

  let p1 = Planner::new(
    Planner::bits_to_num(bits![1,0,1]),
    bits![]
  );
  assert_eq!(p1.inputs, bits![]);
  assert_eq!(p1.outputs, bits![1,0,1]);

  let p2 = Planner::new(
    Planner::bits_to_num(bits![0,0,0,1]),
    bits![]
  );
  assert_eq!(p2.inputs, bits![]);
  assert_eq!(p2.outputs, bits![0,0,0,1]);

  let p3 = Planner::new(
    Planner::bits_to_num(bits![]),
    bits![]
  );
  assert_eq!(p3.inputs, bits![]);
  assert_eq!(p3.outputs, bits![]);
}
