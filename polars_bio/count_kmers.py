from polars_bio.polars_bio import py_count_kmer

def count_kmers(
    path: str,
    k: int = 5
) -> dict:
    """
    Count k-mers in a FASTQ file.
 
    Parameters
    ----------
    path : str
        Path to the FASTQ file.
    k : int
        Length of the k-mers.
 
    Returns
    -------
    dict
        Mapping of k-mer string → count.
    """
    return py_count_kmer(path, k)