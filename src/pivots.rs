use point::Point;

pub fn pad<P> (xs: &Vec<P>, n: usize) -> Vec<P> where P: Point {
  let mut len = xs.len();
  let slots = (n-len).min(len-1);
  let mut res = xs.clone();
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
