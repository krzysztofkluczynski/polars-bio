import polars as pl
from polars_bio.polars_bio import py_count_kmer  # Rust extension (compiled)

def count_kmers(
    df: pl.DataFrame,
    k: int = 5,
    column: str = "sequence"
) -> dict:
    """
    Count k-mers from a Polars DataFrame.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame with a column containing DNA sequences.
    k : int
        Length of the k-mers.
    column : str
        Name of the column with DNA sequences (default: "sequence")

    Returns
    -------
    dict
        Mapping of k-mer string → count.
    """
    # Extract column as list of strings
    sequences = df.select(column).collect()[column].to_list()
    return py_count_kmer(sequences, k)