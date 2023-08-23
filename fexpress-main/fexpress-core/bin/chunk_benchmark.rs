use fexpress_core::naive_aggregate_funcs::stdev;
use fexpress_core::partial_agg::PartialAggregate;
use fexpress_core::partial_aggregates::online_standard_deviation::OnlineStandardDeviation;
use fexpress_core::stats::Stats;
use fexpress_core::types::FLOAT;
use rand::Rng;
use std::iter;
use std::time::Instant;

// 1. Generate Random Vector
fn generate_random_vector(n: usize) -> Vec<FLOAT> {
    let mut rng = rand::thread_rng();
    (0..n).map(|_| rng.gen::<FLOAT>()).collect()
}

// 2. Chunk the Vector
fn chunk_vector(vec: &[FLOAT], chunk_size: usize) -> Vec<Vec<FLOAT>> {
    vec.chunks(chunk_size).map(|chunk| chunk.to_vec()).collect()
}

// 3. Calculate Partial Aggregates
fn calculate_partial_aggregates(chunks: &[Vec<FLOAT>]) -> Vec<OnlineStandardDeviation> {
    chunks
        .iter()
        .map(|chunk| {
            let mut agg = OnlineStandardDeviation::new();
            for el in chunk.iter() {
                agg.update(*el);
            }
            agg
        })
        .collect()
}

// 4. Calculate Aggregates with Tumbling Window
fn calculate_aggregates(
    vec: &[FLOAT],
    partial_aggregates: &[OnlineStandardDeviation],
    chunk_size: usize,
    window_size: usize,
    step_size: usize,
) -> Vec<Option<FLOAT>> {
    let mut results = vec![];
    for window_start in (0..vec.len()).step_by(step_size) {
        let window_end = window_start + window_size;
        if window_end > vec.len() {
            break;
        }

        let mut aggregate = OnlineStandardDeviation::new();

        let chunk_start_idx = window_start / chunk_size;
        let chunk_end_idx = (window_end - 1) / chunk_size;
        for idx in chunk_start_idx + 1..chunk_end_idx {
            aggregate = aggregate.merge(&partial_aggregates[idx]);
        }

        let chunk_start = chunk_start_idx * chunk_size;
        let chunk_end = chunk_end_idx * chunk_size;

        for i in window_start..chunk_start + chunk_size.min(window_end - chunk_start) {
            aggregate.update(vec[i]);
        }

        for i in chunk_end..window_end {
            aggregate.update(vec[i]);
        }

        results.push(aggregate.evaluate());
    }

    results
}

fn calculate_aggregates_no_chunking(
    vec: &[FLOAT],
    window_size: usize,
    step_size: usize,
) -> Vec<FLOAT> {
    (0..vec.len())
        .step_by(step_size)
        .filter_map(|window_start| {
            let window_end = window_start + window_size;
            if window_end <= vec.len() {
                Some(vec[window_start..window_end].std_dev())
            } else {
                None
            }
        })
        .collect()
}

// 5. Evaluate Chunk Sizes
fn evaluate_chunk_sizes(
    vec: &[FLOAT],
    chunk_sizes: &[usize],
    window_size: usize,
    step_size: usize,
) {
    let start_time_no_chunking = Instant::now();
    let aggregates_no_chunking = calculate_aggregates_no_chunking(vec, window_size, step_size);
    let duration_no_chunking = start_time_no_chunking.elapsed();
    println!("No chunking: time taken: {:?}", duration_no_chunking);

    for &chunk_size in chunk_sizes {
        let start_time = Instant::now();

        let chunks = chunk_vector(vec, chunk_size);
        let partial_aggregates = calculate_partial_aggregates(&chunks);
        let agg =
            calculate_aggregates(vec, &partial_aggregates, chunk_size, window_size, step_size);
        let duration = start_time.elapsed();
        println!(
            "Chunk size {}: time taken: {:?} stddev: {:?}",
            chunk_size, duration, agg[0]
        );
        // Evaluate the aggregates here, if necessary
    }
}

pub fn main() {
    let n = 100000;
    let vec = generate_random_vector(n);
    let chunk_sizes: Vec<usize> = vec![
        1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000, 1250, 1500, 1750, 1900,
    ];
    let window_size = 2000;
    let step_size = 200;
    evaluate_chunk_sizes(&vec, &chunk_sizes, window_size, step_size);
}
