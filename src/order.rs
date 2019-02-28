pub fn pivot_order (bf: usize) -> Vec<usize> {
  assert_eq!(
    (1 << (31 - (bf as u32).leading_zeros()))+1,
    bf,
    "branch factor must be a power of 2 plus 1"
  );
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
