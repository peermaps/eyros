use eyros::{Mix,Mix2,Mix3};
use failure::Error;

#[test]
fn mix_into() -> Result<(),Error> {
  let m2 = Mix2::new(Mix::Scalar(3.0_f32),Mix::Interval(45_u16, 500_u16));
  let t2: (Mix<f32>,Mix<u16>) = m2.into();
  assert_eq![
    t2,
    (Mix::Scalar(3.0_f32),Mix::Interval(45_u16, 500_u16))
  ];
  let m3 = Mix3::new(
    Mix::Interval(2.0_f32,8.0_f32),
    Mix::Scalar(5.0_f32),
    Mix::Interval(5u8,7u8),
  );
  let t3: (Mix<f32>,Mix<f32>,Mix<u8>) = m3.into();
  assert_eq![
    t3,
    (
      Mix::Interval(2.0_f32,8.0_f32),
      Mix::Scalar(5.0_f32),
      Mix::Interval(5u8,7u8),
    )
  ];
  Ok(())
}

#[test]
fn mix_from() -> Result<(),Error> {
  {
    type P = Mix2<f32,u16>;
    let t = (Mix::Scalar(3.0_f32),Mix::Interval(45_u16, 500_u16));
    let m2: P = t.into();
    assert_eq![
      m2,
      Mix2::new(Mix::Scalar(3.0_f32),Mix::Interval(45_u16, 500_u16))
    ];
  }
  {
    type P = Mix3<f32,f32,u8>;
    let t = (
      Mix::Interval(2.0_f32,8.0_f32),
      Mix::Scalar(5.0_f32),
      Mix::Interval(5u8,7u8),
    );
    let m3: P = t.into();
    assert_eq![
      m3,
      Mix3::new(
        Mix::Interval(2.0_f32,8.0_f32),
        Mix::Scalar(5.0_f32),
        Mix::Interval(5u8,7u8),
      ).into(),
    ];
  }
  Ok(())
}
