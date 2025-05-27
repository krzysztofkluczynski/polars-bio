import polars as pl
from polars_bio.polars_bio import py_count_kmer  # Rust -> dict

def count_kmers(
    df: pl.LazyFrame,
    k: int = 5
) -> pl.DataFrame:
    """
    Count k-mers from a Polars LazyFrame.

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
    sequences = df.select("sequence").collect()["sequence"].to_list()
    raw_counts = py_count_kmer(sequences, k)

    return pl.DataFrame({
        "kmer": list(raw_counts.keys()),
        "count": list(raw_counts.values())
    })