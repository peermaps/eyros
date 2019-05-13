use crate::point::Point;

pub fn pad<P> (xs: &Vec<P>, n: usize) -> Vec<P> where P: Point {
  let mut len = xs.len();
  if len == 0 {
    panic!["attempted to pad pivots from an empty source"];
  }
  let mut res = xs.clone();
  if len == 1 {
    let r = res[0];
    for _ in 1..n {
      res.push(r.clone());
    }
    return res
  }
  let slots = (n-len).min(len-1);
  for i in 0..slots {
    let k = slots-i-1;
    let j = k*len/slots;
    let x = res[j].midpoint_upper(&res[j+1]);
    res.insert(j+1, x);
    len += 1;
  }
  if res.len() == n { res }
  else { pad(&res, n) }
}
