pub fn plan (a: &Vec<bool>, b: &Vec<bool>)
-> Vec<(usize,Vec<usize>,Vec<usize>)> {
  let len = a.len().max(b.len());
  let mut out = vec![];
  let mut acc = (0,vec![],vec![]);
  let mut carry = 0;
  for i in 0..len {
    let xa = if i < a.len() { a[i] as u64 } else { 0 };
    let xb = if i < b.len() { b[i] as u64 } else { 0 };
    let n = xa + xb + carry;
    let bit = n % 2 == 1;

    if xa == 1 { acc.1.push(i) }
    if xb == 1 && !bit { acc.2.push(i) }

    if bit && xb == 0 {
      acc.0 = i;
      out.push(acc);
      acc = (0,vec![],vec![]);
    }
    carry = n / 2;
  }
  out
}
