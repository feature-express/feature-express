#[cfg(feature = "use_smol_str")]
pub use smol_str::SmolStr as SmallString;

#[cfg(feature = "use_kstring")]
pub use kstring::SmallString;

#[cfg(all(not(feature = "use_smol_str"), not(feature = "use_kstring")))]
pub use std::string::String as SmallString;

macro_rules! from_string {
    ($str:expr) => {{
        #[cfg(feature = "use_smol_str")]
        {
            smol_str::SmolStr::new($str)
        }
        #[cfg(feature = "use_kstring")]
        {
            kstring::KString::from($str)
        }
        #[cfg(all(not(feature = "use_smol_str"), not(feature = "use_kstring")))]
        {
            $str.to_string()
        }
    }};
}

macro_rules! from_str {
    ($str:expr) => {{
        #[cfg(feature = "use_smol_str")]
        {
            smol_str::SmolStr::new($str)
        }
        #[cfg(feature = "use_kstring")]
        {
            kstring::KString::from_ref($str)
        }
        #[cfg(all(not(feature = "use_smol_str"), not(feature = "use_kstring")))]
        {
            $str.to_string()
        }
    }};
}
