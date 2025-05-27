import polars_bio as pb
import pandas as pd
from polars_bio.io import read_fastq


df = read_fastq("tests/data/io/fastq/test.fastq")
print(df)
print(df.schema)
counts = pb.count_kmers(df, k=3)


print(counts)


