pub fn pivot_order (bf: usize) -> Vec<usize> {
  let n = bf*2-3;
  let mut order = Vec::with_capacity(n);
  for i in 0..((((n+1) as f32).log2()) as usize) {
    let m = 2usize.pow(i as u32);
    for j in 0..m {
      order.push(n/(m*2) + j*(n+1)/m);
    }
  }
  order
}
