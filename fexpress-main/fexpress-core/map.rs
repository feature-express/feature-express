#[macro_use]
#[cfg(feature = "use_hashbrown")]
pub use hashbrown::{HashSet, HashMap};

#[cfg(feature = "use_hashbrown")]
pub use hashbrown::hash_map::Entry;

#[cfg(feature = "use_hashbrown")]
macro_rules! hashmap {
    ($( $key: expr => $val: expr ),*) => {{
         let mut map = ::hashbrown::HashMap::new();
         $( map.insert($key, $val); )*
         map
    }}
}

#[cfg(not(feature = "use_hashbrown"))]
pub use std::collections::{HashMap, HashSet};

#[cfg(not(feature = "use_hashbrown"))]
pub use std::collections::hash_map::Entry;

#[cfg(not(feature = "use_hashbrown"))]
macro_rules! hashmap {
    ($( $key: expr => $val: expr ),*) => {{
         let mut map = ::std::collections::HashMap::new();
         $( map.insert($key, $val); )*
         map
    }}
}
