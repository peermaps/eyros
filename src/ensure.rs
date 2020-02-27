#[doc(hidden)]
#[macro_export]
macro_rules! ensure_eq {
  ($left:expr, $right:expr) => ({
    match (&$left, &$right) {
      (left_val, right_val) => {
        if !(*left_val == *right_val) {
          return Err(format_err!(r#"assertion failed: `(left == right)`
  left: `{:?}`,
 right: `{:?}`"#, left_val, right_val));
        }
      }
    }
  });
  ($left:expr, $right:expr, $($arg:tt)*) => ({
    match (&($left), &($right)) {
      (left_val, right_val) => {
        if !(*left_val == *right_val) {
          return Err(format_err!(r#"assertion failed: `(left == right)`
  left: `{:?}`,
 right: `{:?}`: {}"#, left_val, right_val, format_args!($($arg)*)));
        }
      }
    }
  });
}

#[doc(hidden)]
#[macro_export]
macro_rules! ensure_eq_some {
  ($left:expr, $right:expr) => ({
    match (&$left, &$right) {
      (left_val, right_val) => {
        if !(*left_val == *right_val) {
          return Some(Err(format_err!(r#"assertion failed: `(left == right)`
  left: `{:?}`,
 right: `{:?}`"#, left_val, right_val)));
        }
      }
    }
  });
  ($left:expr, $right:expr, $($arg:tt)*) => ({
    match (&($left), &($right)) {
      (left_val, right_val) => {
        if !(*left_val == *right_val) {
          return Some(Err(format_err!(r#"assertion failed: `(left == right)`
  left: `{:?}`,
 right: `{:?}`: {}"#, left_val, right_val, format_args!($($arg)*))));
        }
      }
    }
  });
}
