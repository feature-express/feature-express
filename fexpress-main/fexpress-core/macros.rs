macro_rules! btreemap {
    ($( $key: expr => $val: expr ),*) => {{
         let mut map = std::collections::BTreeMap::new();
         $( map.insert($key, $val); )*
         map
    }}
}

macro_rules! a {
    ($x:expr) => {
        crate::event::AttributeName::new($x)
    };
}
