pub fn transpose_vv<T>(v: Vec<Vec<T>>) -> Vec<Vec<T>> {
    if v.is_empty() {
        return v;
    }
    let len = v[0].len();
    let mut iters: Vec<_> = v.into_iter().map(|n| n.into_iter()).collect();
    (0..len)
        .map(|_| {
            iters
                .iter_mut()
                .map(|n| n.next().unwrap())
                .collect::<Vec<T>>()
        })
        .collect()
}

pub fn transpose_vv1<T: Clone>(v: Vec<Vec<T>>) -> Vec<Vec<T>> {
    if v.is_empty() || v[0].is_empty() {
        return v;
    }

    let mut transposed = vec![vec![v[0][0].clone(); v.len()]; v[0].len()];

    for (i, row) in v.iter().enumerate() {
        for (j, value) in row.iter().enumerate() {
            transposed[j][i] = value.clone();
        }
    }

    transposed
}

pub fn transpose_vv2<T: Clone>(v: Vec<Vec<T>>) -> Vec<Vec<T>> {
    if v.is_empty() {
        return v;
    }

    let mut transposed = vec![vec![v[0][0].clone(); v.len()]; v[0].len()];

    for i in 0..v.len() {
        for j in 0..v[0].len() {
            transposed[j][i] = v[i][j].clone();
        }
    }

    transposed
}

pub fn transpose_vv3<T: Clone>(v: Vec<Vec<T>>) -> Vec<Vec<T>> {
    if v.is_empty() {
        return v;
    }

    (0..v[0].len())
        .map(|j| v.iter().map(|i| i[j].clone()).collect::<Vec<T>>())
        .collect::<Vec<Vec<T>>>()
}

pub fn transpose_vv4<T>(v: Vec<Vec<T>>) -> Vec<Vec<T>> {
    if v.is_empty() {
        return v;
    }
    let len = v[0].len();
    let mut iters: Vec<_> = v.into_iter().map(|n| n.into_iter()).collect();
    (0..len)
        .map(|_| {
            iters
                .iter_mut()
                .filter_map(|n| n.next())
                .collect::<Vec<T>>()
        })
        .collect()
}

#[cfg(tests)]
mod tests {
    #[test]
    fn test_transpose_vv() {
        let v1 = vec![vec![1, 2], vec![3, 4], vec![5, 6]];
        let expected1 = vec![vec![1, 3, 5], vec![2, 4, 6]];
        assert_eq!(transpose_vv(v1), expected1);

        let v2 = vec![vec![1], vec![2], vec![3]];
        let expected2 = vec![vec![1, 2, 3]];
        assert_eq!(transpose_vv(v2), expected2);

        let v3 = vec![vec!["a", "b", "c"], vec!["d", "e", "f"]];
        let expected3 = vec![vec!["a", "d"], vec!["b", "e"], vec!["c", "f"]];
        assert_eq!(transpose_vv(v3), expected3);

        let v4: Vec<Vec<u32>> = Vec::new();
        assert!(transpose_vv(v4).is_empty());
    }
}
