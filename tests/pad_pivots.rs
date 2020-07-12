#![recursion_limit="1024"]

#[path="../src/ensure.rs"]
#[macro_use] mod ensure;

#[path="../src/order.rs"]
mod order;

#[path="../src/point.rs"]
mod point;

#[path="../src/pivots.rs"]
mod pivots;

macro_rules! vp {
  ($($x:expr),*) => {
    {
      let xs: Vec<(f32,f32)> = vec![$(($x,0f32)),*];
      xs
    }
  }
}

#[test]
fn pad_pivots () {
  assert_eq!(
    pivots::pad(&vp![0.0,1.0,2.0,3.0,4.0],8),
    vp![0.0,0.5,1.0,2.0,2.5,3.0,3.5,4.0]
  );
  assert_eq!(
    pivots::pad(&vp![0.0,100.0],8),
    vp![0.0,12.5,25.0,50.0,62.5,75.0,87.5,100.0]
  );
  assert_eq!(
    pivots::pad(&vp![0.0,1.0,2.0],4),
    vp![0.0,0.5,1.0,2.0]
  );
  assert_eq!(
    pivots::pad(&vp![1.0,2.0,3.0],4),
    vp![1.0,1.5,2.0,3.0]
  );
  assert_eq!(
    pivots::pad(&vp![0.0,1.0,2.0],10),
    vp![0.0,0.125,0.25,0.5,0.75,1.0,1.5,1.625,1.75,2.0]
  );
  assert_eq!(
    pivots::pad(&vp![0.0],5),
    vp![0.0,0.0,0.0,0.0,0.0]
  );
}
