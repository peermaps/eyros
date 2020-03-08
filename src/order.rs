pub fn order (bf: usize, i: usize) -> usize {
  assert_eq![
    (1 << (31 - (bf as u32).leading_zeros()))+1,
    bf,
    "branch factor must be a power of 2 plus 1"
  ];
  let n = order_len(bf);
  assert![
    i < n,
    "index out of bounds for branch_factor={}, i={}, n={}",
    bf, i, n
  ];
  let b = 0usize.leading_zeros() - (i+1).leading_zeros() - 1;
  let j = i+1-2usize.pow(b);
  let m = 2usize.pow(b);
  n/(m*2) + j*(n+1)/m
}

pub fn order_len (bf: usize) -> usize { bf*2-3 }
