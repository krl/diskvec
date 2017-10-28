pub trait Volatile
where
    Self: Copy + PartialEq,
{
    const ZEROED: Self;
}
