use crate::map::HashMap;
use std::cmp::Eq;
use std::cmp::Ord;
use std::hash::Hash;

pub fn intersect<T, R>(vec: Vec<Option<Vec<(T, Vec<R>)>>>) -> Option<Vec<(T, Vec<R>)>>
where
    T: Hash + Eq + Clone + Ord,
    R: Hash + Clone + Ord,
{
    // If the input vector is empty, return None immediately
    if vec.is_empty() {
        return None;
    }

    // If the input vector has only one element, return that element
    if vec.len() == 1 {
        if let Some(first) = vec.first() {
            return first.clone();
        }
    }

    // If there are any None elements, return None immediately
    let vec: Vec<_> = vec.into_iter().flatten().collect();
    if vec.is_empty() {
        return None;
    }

    let mut counter: HashMap<(T, R), usize> = HashMap::new();

    // Unwrap is safe here as we've already checked for None elements
    for vecinner in vec.iter() {
        for (t, sv) in vecinner.iter() {
            for s in sv.iter() {
                counter
                    .entry((t.clone(), s.clone()))
                    .and_modify(|e| *e += 1)
                    .or_insert(1);
            }
        }
    }

    // Construct hashmap for final result
    let mut result_map: HashMap<T, Vec<R>> = HashMap::new();
    for ((t, s), count) in counter.into_iter() {
        if count == vec.len() {
            result_map.entry(t).or_insert_with(Vec::new).push(s);
        }
    }

    // Construct final result
    let mut result: Vec<_> = result_map.into_iter().collect();

    // Sort outer vector
    result.sort_unstable();

    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_single() {
        let input = vec![Some(vec![
            (1, vec![Arc::new(2), Arc::new(3), Arc::new(4)]),
            (2, vec![Arc::new(3)]),
        ])];

        assert_eq!(
            intersect(input.clone()),
            Some(vec![
                (1, vec![Arc::new(2), Arc::new(3), Arc::new(4)]),
                (2, vec![Arc::new(3)]),
            ])
        );
    }

    #[test]
    fn test_double() {
        let input = vec![
            Some(vec![
                (1, vec![Arc::new(2), Arc::new(3), Arc::new(4)]),
                (2, vec![Arc::new(3), Arc::new(4)]),
            ]),
            Some(vec![(1, vec![Arc::new(2)]), (2, vec![Arc::new(4)])]),
        ];

        assert_eq!(
            intersect(input),
            Some(vec![(1, vec![Arc::new(2)]), (2, vec![Arc::new(4)])])
        );
    }

    #[test]
    fn test_triple() {
        let input = vec![
            Some(vec![
                (1, vec![Arc::new(2), Arc::new(3)]),
                (2, vec![Arc::new(3), Arc::new(4)]),
            ]),
            Some(vec![(1, vec![Arc::new(2)]), (2, vec![Arc::new(4)])]),
            Some(vec![(1, vec![Arc::new(2)]), (2, vec![Arc::new(4)])]),
        ];

        assert_eq!(
            intersect(input),
            Some(vec![(1, vec![Arc::new(2)]), (2, vec![Arc::new(4)])])
        );
    }

    #[test]
    fn test_empty_single() {
        let input = vec![Some(vec![])];

        assert_eq!(intersect::<i32, i32>(input), Some(vec![]));
    }

    #[test]
    fn test_empty_double() {
        let input = vec![Some(vec![]), Some(vec![])];

        assert_eq!(intersect::<i32, i32>(input), None);
    }

    #[test]
    fn test_empty_triple() {
        let input = vec![Some(vec![]), Some(vec![]), Some(vec![])];

        assert_eq!(intersect::<i32, i32>(input), None);
    }
}
