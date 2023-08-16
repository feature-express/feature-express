use petgraph::visit::Walker;

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

#[derive(Debug, Clone)]
struct WeightedNode {
    index: usize,
    weight: usize,
    depth: usize,
}

pub struct WeightedDirectedGraph<N: Eq + Clone + std::hash::Hash> {
    nodes: HashMap<N, usize>,
    nodes_rev: HashMap<usize, N>,
    edges: Vec<HashSet<usize>>,
    weights: Vec<usize>,
}

impl<N: Eq + Clone + std::hash::Hash + Debug> WeightedDirectedGraph<N> {
    pub fn new() -> Self {
        WeightedDirectedGraph {
            nodes: HashMap::new(),
            nodes_rev: HashMap::new(),
            edges: Vec::new(),
            weights: Vec::new(),
        }
    }

    pub fn add_edge(&mut self, source: N, source_weight: usize, target: N, target_weight: usize) {
        let src_idx = self.add_node(source, source_weight);
        let tgt_idx = self.add_node(target, target_weight);

        self.edges[src_idx].insert(tgt_idx);
    }

    fn add_node(&mut self, node: N, weight: usize) -> usize {
        if let Some(&index) = self.nodes.get(&node) {
            return index;
        }
        let index = self.edges.len();
        self.nodes.insert(node.clone(), index);
        self.nodes_rev.insert(index, node);
        self.edges.push(HashSet::new());
        self.weights.push(weight);
        index
    }

    fn calculate_depth(&self, node_index: usize, depths: &mut Vec<Option<usize>>) -> usize {
        if let Some(cached_depth) = depths[node_index] {
            return cached_depth;
        }

        let mut max_depth = 0;
        for &next_node_index in &self.edges[node_index] {
            max_depth = std::cmp::max(max_depth, 1 + self.calculate_depth(next_node_index, depths));
        }

        depths[node_index] = Some(max_depth);
        max_depth
    }

    fn calculate_immediate_dependents_depth(
        &self,
        node_index: usize,
        depths: &Vec<Option<usize>>,
    ) -> usize {
        let mut max_immediate_dependents_depth = 0;
        for &next_node_index in &self.edges[node_index] {
            max_immediate_dependents_depth = std::cmp::max(
                max_immediate_dependents_depth,
                depths[next_node_index].unwrap_or(0),
            );
        }
        max_immediate_dependents_depth
    }

    pub fn topo_sort_weighted(&self, ascending: bool) -> Option<Vec<N>> {
        let mut in_degree = vec![0; self.edges.len()];
        let mut weighted_nodes = Vec::new();
        let mut depths = vec![None; self.edges.len()];

        for edges in &self.edges {
            for &tgt_idx in edges {
                in_degree[tgt_idx] += 1;
            }
        }

        for (idx, &degree) in in_degree.iter().enumerate() {
            if degree == 0 {
                let node_weight = self.weights[idx];
                let depth = self.calculate_depth(idx, &mut depths);
                weighted_nodes.push(WeightedNode {
                    index: idx,
                    weight: node_weight,
                    depth: depth,
                });
            }
        }

        let mut topo_order = Vec::new();

        while let Some(WeightedNode { index: u, .. }) = weighted_nodes.pop() {
            if let Some(node) = self.nodes.iter().find(|&(_, &v)| v == u) {
                topo_order.push(node.0.clone());
            }

            for &v in &self.edges[u] {
                in_degree[v] -= 1;
                if in_degree[v] == 0 {
                    let node_weight = self.weights[v];
                    let depth = self.calculate_depth(v, &mut depths);
                    weighted_nodes.push(WeightedNode {
                        index: v,
                        weight: node_weight,
                        depth: depth,
                    });
                }
            }

            weighted_nodes.sort_by(|a, b| {
                let depth_order = b.depth.cmp(&a.depth); // Note: reversed for depth (we want descending)
                let weight_order = if ascending {
                    a.weight.cmp(&b.weight)
                } else {
                    b.weight.cmp(&a.weight)
                };
                depth_order.then(weight_order)
            });
        }

        if topo_order.len() == self.nodes.len() {
            Some(topo_order)
        } else {
            None
        }
    }

    pub fn augmented_topo_sort_weighted(&self, ascending: bool) -> Option<Vec<(N, Vec<N>)>> {
        let mut in_degree = vec![0; self.nodes.len()];
        let mut ref_count = vec![0; self.nodes.len()];
        let mut predecessors: Vec<HashSet<usize>> = vec![HashSet::new(); self.nodes.len()];
        let mut weighted_nodes = Vec::new();
        let mut depths = vec![None; self.edges.len()];

        for (idx, edges) in self.edges.iter().enumerate() {
            ref_count[idx] = edges.len();
            for &tgt_idx in edges {
                in_degree[tgt_idx] += 1;
                predecessors[tgt_idx].insert(idx);
            }
        }

        for (idx, &degree) in in_degree.iter().enumerate() {
            if degree == 0 {
                let node_weight = self.weights[idx];
                let depth = self.calculate_depth(idx, &mut depths);
                weighted_nodes.push(WeightedNode {
                    index: idx,
                    weight: node_weight,
                    depth: depth,
                });
            }
        }

        let mut topo_order = Vec::new();

        while let Some(WeightedNode { index: u, .. }) = weighted_nodes.pop() {
            if let Some(node) = self.nodes.iter().find(|&(_, &v)| v == u) {
                let mut retired = vec![];
                for predecessor in predecessors[*node.1].iter() {
                    ref_count[*predecessor] -= 1;
                    if ref_count[*predecessor] == 0 {
                        retired.push(self.nodes_rev[predecessor].clone())
                    }
                }
                topo_order.push((node.0.clone(), retired));
            }

            for &v in &self.edges[u] {
                in_degree[v] -= 1;
                if in_degree[v] == 0 {
                    let node_weight = self.weights[v];
                    let depth = self.calculate_depth(v, &mut depths);
                    weighted_nodes.push(WeightedNode {
                        index: v,
                        weight: node_weight,
                        depth: depth,
                    });
                }
            }

            weighted_nodes.sort_by(|a, b| {
                let depth_order = b.depth.cmp(&a.depth); // Note: reversed for depth (we want descending)
                let weight_order = if ascending {
                    a.weight.cmp(&b.weight)
                } else {
                    b.weight.cmp(&a.weight)
                };
                depth_order.then(weight_order)
            });
        }

        if topo_order.len() == self.nodes.len() {
            Some(topo_order)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_augmented_topo_sort_weighted() {
        let mut graph = WeightedDirectedGraph::new();
        graph.add_edge('C', 1, 'D', 1);
        graph.add_edge('D', 1, 'E', 1);
        graph.add_edge('B', 1, 'E', 1);
        graph.add_edge('E', 1, 'F', 1);

        let topo_sort = graph.augmented_topo_sort_weighted(true);

        let expected_1 = vec![
            ('B', vec![]),
            ('C', vec![]),
            ('D', vec!['C']),
            ('E', vec!['D', 'B']),
            ('F', vec!['E']),
        ];

        let expected_2 = vec![
            ('B', vec![]),
            ('C', vec![]),
            ('D', vec!['C']),
            ('E', vec!['B', 'D']),
            ('F', vec!['E']),
        ];
        assert!(topo_sort == Some(expected_1) || topo_sort == Some(expected_2));
    }

    #[test]
    fn test_empty_graph() {
        let graph = WeightedDirectedGraph::<char>::new();
        let topo_sort = graph.augmented_topo_sort_weighted(true);
        assert_eq!(topo_sort, Some(vec![]));
    }
}
