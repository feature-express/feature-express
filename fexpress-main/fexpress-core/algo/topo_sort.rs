use std::collections::{HashMap, VecDeque};

pub fn topological_sort(nodes: Vec<usize>, edges: Vec<(usize, usize)>) -> Option<Vec<usize>> {
    let mut graph = HashMap::new();
    let mut incoming_edges = vec![0; nodes.len()];

    for &(from, to) in &edges {
        graph.entry(from).or_insert_with(Vec::new).push(to);
        incoming_edges[to] += 1;
    }

    let mut queue = VecDeque::new();
    for &node in &nodes {
        if incoming_edges[node] == 0 {
            queue.push_back(node);
        }
    }

    let mut result = Vec::new();
    while let Some(node) = queue.pop_front() {
        result.push(node);

        if let Some(neighbors) = graph.get(&node) {
            for &neighbor in neighbors {
                incoming_edges[neighbor] -= 1;
                if incoming_edges[neighbor] == 0 {
                    queue.push_back(neighbor);
                }
            }
        }
    }

    if result.len() == nodes.len() {
        Some(result)
    } else {
        None // there is a cycle
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topological_sort_no_cycle() {
        let nodes = vec![0, 1, 2, 3, 4, 5];
        let edges = vec![(2, 3), (3, 1), (4, 0), (4, 1), (5, 0), (5, 2)];
        let sorted = topological_sort(nodes, edges).unwrap();
        // Verify that every edge (u, v) satisfies the condition index(u) < index(v)
        let edge_conditions = vec![(2, 3), (3, 1), (4, 0), (4, 1), (5, 0), (5, 2)]
            .into_iter()
            .all(|(u, v)| {
                sorted.iter().position(|&x| x == u) < sorted.iter().position(|&x| x == v)
            });
        assert!(edge_conditions, "Not properly sorted");
    }

    #[test]
    fn test_topological_sort_with_cycle() {
        let nodes = vec![0, 1, 2, 3];
        let edges = vec![(0, 1), (1, 2), (2, 3), (3, 0)];
        assert!(
            topological_sort(nodes, edges).is_none(),
            "Should detect a cycle"
        );
    }

    #[test]
    fn test_topological_sort_empty_graph() {
        let nodes = vec![];
        let edges = vec![];
        let sorted = topological_sort(nodes, edges).unwrap();
        assert_eq!(sorted.len(), 0, "Sorted list should be empty");
    }
}
