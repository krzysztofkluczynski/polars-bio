import polars as pl
from polars_bio.polars_bio import py_count_kmer_mt  # Rust multithreaded kmer count

def count_kmers_mt(
    df: pl.LazyFrame,
    k: int = 5,
    column: str = "sequence"
) -> pl.DataFrame:
    """
    Count k-mers from a Polars LazyFrame using multi-threaded Rust backend.

    Parameters
    ----------
    df : pl.LazyFrame
        LazyFrame with a column containing DNA sequences.
    k : int
        Length of the k-mers.
    column : str
        Name of the column with DNA sequences (default: "sequence")

    Returns
    -------
    pl.DataFrame
        DataFrame with two columns: kmer (str), count (int)
    """
    sequences = df.select(column).collect()[column].to_list()
    raw_counts = py_count_kmer_mt(sequences, k)

    return pl.DataFrame({
        "kmer": list(raw_counts.keys()),
        "count": list(raw_counts.values())
    })