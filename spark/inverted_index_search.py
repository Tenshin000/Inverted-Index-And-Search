import argparse
import re
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    input_file_name,
    lower,
    regexp_replace,
    split,
    explode,
    col,
    count as sql_count,
    collect_list,
    concat_ws
)

#----------------------------#
#        CONFIGURATION       #
#----------------------------#
HDFS_BASE   = 'hdfs:///user/hadoop/'
DATA_DIR    = HDFS_BASE + 'inverted-index/data'
OUTPUT_BASE = HDFS_BASE + 'inverted-index/'
DEFAULT_SHUFFLE_PARTITIONS = 48

#----------------------------#
#   HDFS UTILITY FUNCTIONS   #
#----------------------------#
def hdfs_dir_exists(spark_context, path):
    """Check if an HDFS path exists using Hadoop API."""
    jvm = spark_context._jvm
    hadoop_conf = spark_context._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))

def list_hdfs_files(spark_context, path):
    """Return list of (path, size) tuples for files in HDFS directory."""
    jvm = spark_context._jvm
    hadoop_conf = spark_context._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    p = jvm.org.apache.hadoop.fs.Path(path)
    files = []
    if fs.exists(p):
        for f in fs.listStatus(p):
            if f.isFile():
                files.append((f.getPath().toString(), f.getLen()))
    return files

def choose_input_paths(sc, limit_mb=None):
    if not hdfs_dir_exists(sc, DATA_DIR):
        print(f"Data directory {DATA_DIR} does not exist.")
        return []
    if limit_mb is None:
        return [DATA_DIR + '/*']
    mb = limit_mb * 1024 * 1024
    files = list_hdfs_files(sc, DATA_DIR)
    selected, total = [], 0
    for p, sz in sorted(files, key=lambda x: x[1]):
        if total + sz > mb:
            break
        selected.append(p)
        total += sz
    return selected

def choose_output_path(sc):
    base = OUTPUT_BASE + 'output'
    idx = ''
    while hdfs_dir_exists(sc, base + idx):
        idx = str(int(idx or '0') + 1)
    return base + idx

#----------------------------#
#         SPARK JOB          #
#----------------------------#
class InvertedIndexSearch:
    def __init__(self, app_name="InvertedIndexSearch"):
        conf = SparkConf().setAppName(app_name).set("spark.sql.shuffle.partitions", str(DEFAULT_SHUFFLE_PARTITIONS))
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.sc = self.spark.sparkContext

    def build_index(self, input_paths, output_path, num_partitions=None):
        """PHASE 1: MAP - Tokenization and Preprocessing"""
        # Read files with filename column
        df = (self.spark
              .read 
              .text(input_paths) # Read all text files from input_paths
              .withColumn("filename", regexp_replace(input_file_name(), r"hdfs://[^/]+/user/hadoop/inverted-index/data/", "")))
            # withColumn adds filename column removing HDFS prefix, so only filename remains.

        # Tokenize and clean text
        tokens = (df
                  # Replaces everything that is not a letter or number with spaces and converts it to lowercase
                    .withColumn("clean", lower(regexp_replace(col("value"), "[\\W_]+", " "))) 
                  # Transform each row into many records, one per word
                    .withColumn("word", explode(split(col("clean"), "\\s+")))
                  # filter removes any empty strings
                    .filter(col("word") != ""))
        
        """PHASE 2: REDUCE - Occurrence counting"""
        # Count word occurrences per file
        counts = (tokens
                  .groupBy("word", "filename")
                  .agg(sql_count("*").alias("cnt")))

        """FASE 3: MAP - postings list"""
        # Create postings list and format output
        postings = (counts
                    .groupBy("word")
                    # For each word, it collects all the "filename:count" strings into a list
                    .agg(collect_list(concat_ws(":", col("filename"), col("cnt"))).alias("file_counts"))
                    # Concatenate them with tabs between them, obtaining the list of documents and counts
                    .select(col("word"), concat_ws("\t", col("file_counts")).alias("postings")))

        """PHASE 4: REDUCE - Final formatting and sorting"""
        # Format to final output lines
        formatted = postings \
            .orderBy("word") \
            .rdd \
            .map(lambda r: f"{r.word}\t{r.postings}")
        # Sort words alphabetically.
        # Go from DataFrame to RDD 
        # Map each record to a tab-delimited string.

        # Write output
        partitions = num_partitions or self.sc.defaultParallelism # Repartition according to the chosen number of partitions
        formatted.repartition(partitions).saveAsTextFile(output_path) # Save to HDFS in output_path

    def stop(self):
        self.sc.stop()

#----------------------------#
#            MAIN            #
#----------------------------#
def main():
    parser = argparse.ArgumentParser(description="Spark Inverted Index Builder")
    parser.add_argument('--num-partitions', type=int, help="Override number of output partitions")
    known, unknown = parser.parse_known_args()

    # Parse optional --<N>MB flag
    limit = None
    for arg in unknown:
        m = re.match(r'--(\d+)MB$', arg)
        if m:
            limit = int(m.group(1))
            break

    engine = InvertedIndexSearch()
    try:
        inputs = choose_input_paths(engine.sc, limit)
        if not inputs:
            sys.exit(1)
        output_path = choose_output_path(engine.sc)
        engine.build_index(inputs, output_path, num_partitions=known.num_partitions)
        print(f"Index saved to {output_path}")
    finally:
        engine.stop()

if __name__ == "__main__":
    main()
