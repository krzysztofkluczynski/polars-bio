import polars_bio as pb
import pandas as pd
from polars_bio.io import read_fastq


df1 = pd.DataFrame([
    ['chr1', 1, 5],
    ['chr1', 3, 8],
    ['chr1', 8, 10],
    ['chr1', 12, 14]],
columns=['chrom', 'start', 'end']
)

df2 = pd.DataFrame(
[['chr1', 4, 8],
['chr1', 10, 11]],
columns=['chrom', 'start', 'end' ]
)
counts = pb.count_overlaps(df1, df2, output_type="pandas.DataFrame")

df = read_fastq("tests/data/io/fastq/test.fastq")
counts = pb.count_kmers(df, k=3)
print(counts)


