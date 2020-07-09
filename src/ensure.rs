#[doc(hidden)]
#[macro_export]
macro_rules! ensure_eq_box {
  ($left:expr, $right:expr) => ({
    match (&$left, &$right) {
      (left_val, right_val) => {
        if !(*left_val == *right_val) {
          //return Err(format_err!(r#"assertion failed: `(left == right)`
          return Err(Box::new(format_err!(r#"assertion failed: `(left == right)`
  left: `{:?}`,
 right: `{:?}`"#, left_val, right_val).compat()));
        }
      }
    }
  });
  ($left:expr, $right:expr, $($arg:tt)*) => ({
    match (&($left), &($right)) {
      (left_val, right_val) => {
        if !(*left_val == *right_val) {
          //return Err(format_err!(r#"assertion failed: `(left == right)`
          return Err(Box::new(format_err!(r#"assertion failed: `(left == right)`
  left: `{:?}`,
 right: `{:?}`: {}"#, left_val, right_val, format_args!($($arg)*)).compat()));
        }
      }
    }
  });
}

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

#[doc(hidden)]
#[macro_export]
macro_rules! ensure {
  ($x:expr) => ({
    match &($x) {
      x => {
        if !*x {
          return Err(Box::new(format_err!(r#"assertion failed"#).compat()));
          //return Err(format_err!(r#"assertion failed"#));
        }
      }
    }
  });
  ($x:expr, $($arg:tt)*) => ({
    match &($x) {
     x => {
        if !*x {
          return Err(Box::new(format_err!(r#"assertion failed"#).compat()));
          //return Err(format_err!(r#"assertion failed"#));
        }
      }
    }
  });
}

#[doc(hidden)]
#[macro_export]
macro_rules! fail {
  ($($arg:tt)*) => ({
    return Err(Box::new(failure::err_msg(format![$($arg)*]).compat()));
    //return Err(failure::err_msg(format![$($arg)*]));
  });
}
