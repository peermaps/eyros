extern crate eyros;
#[path="../src/order.rs"]
mod order;
use order::pivot_order;

#[test]
fn order () {
  assert_eq!(
    pivot_order(5),
    vec![3,1,5,0,2,4,6],
    "order for branch factor 5"
  );
  assert_eq!(
    pivot_order(9),
    vec![7,3,11,1,5,9,13,0,2,4,6,8,10,12,14],
    "order for branch factor 9"
  );
}
