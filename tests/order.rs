extern crate eyros;
#[path="../src/order.rs"]
mod order;
use order::{order,order_len};

#[test]
fn order_test () {
  assert_eq!(
    {
      let items: Vec<usize> = (0..order_len(5)).map(|i| order(5,i)).collect();
      items
    },
    vec![3,1,5,0,2,4,6],
    "order fn for branch factor 5"
  );
  assert_eq!(
    {
      let items: Vec<usize> = (0..order_len(9)).map(|i| order(9,i)).collect();
      items
    },
    vec![7,3,11,1,5,9,13,0,2,4,6,8,10,12,14],
    "order fn for branch factor 9"
  );
}
