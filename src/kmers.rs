use std::collections::HashMap;
use polars_core::utils::rayon::prelude::ParallelSlice;
use polars_core::utils::rayon::prelude::*;
fn reverse_complement(seq: &[u8]) -> Vec<u8> {
    seq.iter()
        .rev()
        .map(|b| match b {
            b'A' | b'a' => b'T',
            b'T' | b't' => b'A',
            b'C' | b'c' => b'G',
            b'G' | b'g' => b'C',
            _ => b'N',
        })
        .collect()
}

fn canonical_kmer(kmer: &[u8]) -> Vec<u8> {
    let rc = reverse_complement(kmer);
    if rc.as_slice() < kmer {
        rc
    } else {
        kmer.to_vec()
    }
}

pub fn do_count_kmers_mt(sequences: Vec<String>, k: usize) -> HashMap<String, u64> {
    // Use rayon to parallelize counting over chunks of sequences
    let partial_counts: Vec<HashMap<String, u64>> = sequences
        .par_rchunks(1000) // chunk size can be tuned
        .map(|chunk| {
            let mut counts = HashMap::new();
            for seq in chunk {
                let bytes = seq.as_bytes();
                if bytes.len() < k {
                    continue;
                }
                for i in 0..=bytes.len() - k {
                    let kmer = &bytes[i..i + k];
                    if kmer.iter().any(|&b| !matches!(b, b'A' | b'C' | b'G' | b'T' | b'a' | b'c' | b'g' | b't')) {
                        continue;
                    }
                    let canonical = canonical_kmer(kmer);
                    let kmer_str = String::from_utf8_lossy(&canonical).to_string();
                    *counts.entry(kmer_str).or_insert(0) += 1;
                }
            }
            counts
        })
        .collect();

    // Merge all partial HashMaps into one
    let mut merged_counts = HashMap::new();
    for counts in partial_counts {
        for (kmer, count) in counts {
            *merged_counts.entry(kmer).or_insert(0) += count;
        }
    }

    merged_counts
}
