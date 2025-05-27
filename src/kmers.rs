use std::collections::HashMap;

/// Reverse complement of a DNA sequence
fn reverse_complement(seq: &[u8]) -> Vec<u8> {
    seq.iter()
        .rev()
        .map(|b| match b {
            b'A' | b'a' => b'T',
            b'T' | b't' => b'A',
            b'C' | b'c' => b'G',
            b'G' | b'g' => b'C',
            _ => b'N', // Ambiguous base
        })
        .collect()
}

/// Return canonical form of a k-mer: lexicographically smaller of forward and reverse complement
fn canonical_kmer(kmer: &[u8]) -> Vec<u8> {
    let rc = reverse_complement(kmer);
    if rc.as_slice() < kmer {
        rc
    } else {
        kmer.to_vec()
    }
}

/// Count canonical k-mers in given DNA sequences
pub fn do_count_kmers(sequences: Vec<String>, k: usize) -> HashMap<String, u64> {
    let mut counts = HashMap::new();

    for seq in sequences {
        let bytes = seq.as_bytes();
        if bytes.len() < k {
            continue;
        }

        for i in 0..=bytes.len() - k {
            let kmer = &bytes[i..i + k];

            // Skip invalid k-mers with non-ACGT characters
            if kmer.iter().any(|&b| !matches!(b, b'A' | b'C' | b'G' | b'T' | b'a' | b'c' | b'g' | b't')) {
                continue;
            }

            let canonical = canonical_kmer(kmer);
            let kmer_str = String::from_utf8_lossy(&canonical).to_string();
            *counts.entry(kmer_str).or_insert(0) += 1;
        }
    }

    counts
}
