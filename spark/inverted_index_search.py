import argparse
import os
import re
import sys
import subprocess
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    input_file_name,
    lower,
    regexp_replace,
    split,
    explode,
    col,
    count as sql_count
)

#----------------------------#
#        CONFIGURATION       #
#----------------------------#
HDFS_BASE   = 'hdfs:///user/hadoop/'
DATA_DIR    = HDFS_BASE + 'inverted-index/data'
OUTPUT_BASE = HDFS_BASE + 'inverted-index/'
DEFAULT_SHUFFLE_PARTITIONS = 48 # Default number of shuffle partitions
# Set to 48 to handle expensive shuffles. 
# The value was chosen based on the cluster configuration (3 nodes, each with 8 vCPUs and 1 active container), 
# for an estimated total of about 24 contention-free parallel tasks.
# Increasing to 48 allows for better load balancing and reduces the risk of data distribution imbalances between partitions.

#----------------------------#
#   HDFS UTILITY FUNCTIONS   #
#----------------------------#
"""Check if an HDFS directory exists."""
def hdfs_dir_exists(path):
    return subprocess.run(['hdfs', 'dfs', '-test', '-d', path]).returncode == 0

"""List all files (with sizes) under an HDFS directory."""
def list_hdfs_files(path):
    proc = subprocess.run(
        ['hdfs', 'dfs', '-ls', path],
        capture_output=True, text=True, check=True
    )
    files = []
    for line in proc.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 8 and parts[0].startswith('-'):
            size = int(parts[4]); p = parts[7]
            files.append((p, size))
    return files

"""
    Select input files up to a size limit (in MB), or all files if no limit.
    Returns a list of HDFS paths.
"""
def choose_input_paths(limit_mb=None):
    if not hdfs_dir_exists(DATA_DIR):
        print(f"Data directory {DATA_DIR} does not exist. Generate input data first.")
        return []
    if limit_mb is None:
        return [DATA_DIR + '/*']
    mb = limit_mb * 1024 * 1024
    selected, total = [], 0
    for p, sz in sorted(list_hdfs_files(DATA_DIR), key=lambda x: x[1]):
        if total + sz > mb:
            break
        selected.append(p)
        total += sz
    return selected

"""
    Generate a new output path under OUTPUT_BASE/output, 
    appending an index if needed to avoid collisions.
"""
def choose_output_path():
    base = OUTPUT_BASE + 'output'
    idx = ''
    while hdfs_dir_exists(base + idx):
        idx = str(int(idx or '0') + 1)
    return base + idx

#----------------------------#
#         SPARK JOB          #
#----------------------------#
class InvertedIndexSearch:
    def __init__(self, app_name="InvertedIndexSearch"):
        # Configure Spark with tuned shuffle partitions
        conf = SparkConf() \
            .setAppName(app_name) \
            .set("spark.sql.shuffle.partitions", str(DEFAULT_SHUFFLE_PARTITIONS))
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.sc = self.spark.sparkContext

    def build_index(self, input_paths, output_path, num_partitions=None):
        # Read text files and tag each row with its filename
        df = (self.spark
              .read
              .text(input_paths)
              .withColumn("filename", input_file_name()))

        # Tokenize using DataFrame API (lowercase, regex replace, split, explode)
        tokens = (df
                  .withColumn("clean", lower(regexp_replace(col("value"), "[\\W_]+", " ")))
                  .withColumn("word", explode(split(col("clean"), "\\s+")))
                  .filter(col("word") != ""))

        # Count occurrences per (word, filename)
        counts = (tokens
                  .groupBy("word", "filename")
                  .agg(sql_count("*").alias("cnt")))

        # Build inverted index by grouping filename:count entries for each word
        # We do a double group: first count, then reassemble strings in RDD for final formatting
        inverted = (counts
                    .select("word", "filename", "cnt")
                    .orderBy("word", "filename"))

        # Format each line as: word \t file1:count \t file2:count ...
        formatted = (inverted
                     .rdd
                     .map(lambda r: f"{r.word}\t{r.filename}:{r.cnt}")
                     .groupBy(lambda s: s.split("\t")[0])
                     .map(lambda kv: kv[0] + "\t" + "\t".join(
                         [e.split("\t",1)[1] for e in sorted(kv[1])]
                     )))

        # Repartition and save to HDFS
        parts = num_partitions or self.sc.defaultParallelism
        formatted \
            .repartition(parts) \
            .saveAsTextFile(output_path)

    def stop(self):
        self.sc.stop()

#----------------------------#
#            MAIN            #
#----------------------------#
def main():
    parser = argparse.ArgumentParser(description="Spark Inverted Index Builder")
    parser.add_argument('--num-partitions', type=int, help="Override number of output partitions")
    known, unknown = parser.parse_known_args()

    # Parse optional limit flag of the form --<N>MB
    limit = None
    for arg in unknown:
        m = re.match(r'--(\d+)MB$', arg)
        if m:
            limit = int(m.group(1))
            break

    inputs = choose_input_paths(limit)
    if not inputs:
        sys.exit(1)

    output_path = choose_output_path()
    engine = InvertedIndexSearch()
    try:
        engine.build_index(inputs, output_path, num_partitions=known.num_partitions)
        print(f"Index saved to {output_path}")
    finally:
        engine.stop()

if __name__ == "__main__":
    main()
