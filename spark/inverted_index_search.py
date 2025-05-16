import argparse
import os
import re
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name

class InvertedIndexSearch:
    def __init__(self, app_name="InvertedIndexSearch"):
        # Initialize SparkSession and SparkContext
        conf = SparkConf().setAppName(app_name)
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.sc = self.spark.sparkContext

    # Precompile regex pattern for tokenization
    _token_pattern = re.compile(r"[\W_]+")

    @staticmethod
    def tokenize(text):
        # Clean and split text into words
        return InvertedIndexSearch._token_pattern.sub(" ", text.lower()).split()

    def build_index(self, input_paths, output_path, num_partitions=None):
        # Read text files in a distributed manner
        df = self.spark.read.text(input_paths)
        # Add filename column
        df = df.withColumn("filename", input_file_name())

        # Convert DataFrame to RDD of (filename, line_text)
        rdd = df.rdd.map(lambda row: (os.path.basename(row.filename), row.value))

        # Local reference to tokenize
        tokenize = InvertedIndexSearch.tokenize

        # Create ((word, filename), 1) pairs
        pairs = rdd.flatMap(lambda fn_text: [((word, fn_text[0]), 1) for word in tokenize(fn_text[1])])

        # Aggregate counts per (word, filename)
        counts = pairs.reduceByKey(lambda x, y: x + y)

        # Prepare (word, (filename, count)) pairs
        word_file_counts = counts.map(lambda wc: (wc[0][0], (wc[0][1], wc[1])))

        # Combine per word into a dict of {filename: count}
        def seq_op(acc, fv):
            fn, cnt = fv
            acc.setdefault(fn, 0)
            acc[fn] += cnt
            return acc
        def comb_op(a, b):
            for fn, cnt in b.items():
                a.setdefault(fn, 0)
                a[fn] += cnt
            return a

        index_dicts = word_file_counts.combineByKey(
            lambda fv: {fv[0]: fv[1]},
            seq_op,
            comb_op
        )

        # Format postings lists
        formatted = index_dicts.map(
            lambda kv: f"{kv[0]}\t" + ", ".join(f"{fn}:{cnt}" for fn, cnt in sorted(kv[1].items()))
        )

        # Repartition output based on provided or default parallelism
        partitions = num_partitions or self.sc.defaultParallelism
        formatted = formatted.repartition(partitions)
        formatted.saveAsTextFile(output_path)
        print(f"Optimized index saved to {output_path}")

    def stop(self):
        self.sc.stop()


def main():
    parser = argparse.ArgumentParser(description="Optimized Inverted Index with Spark and HDFS support")
    parser.add_argument('inputs', nargs='+', help='Input file(s) or directory names (relative)')
    parser.add_argument('output', help='Output directory name (relative)')
    parser.add_argument('--num-partitions', type=int, help='Number of output partitions/reducers')
    args = parser.parse_args()

    # HDFS absolute prefix
    hdfs_base = 'hdfs://hadoop-namenode:9820/user/hadoop/'

    # Build full HDFS paths as list
    input_paths = [hdfs_base + inp.strip('/') for inp in args.inputs]
    output_path = hdfs_base + args.output.strip('/')

    engine = InvertedIndexSearch()
    try:
        engine.build_index(input_paths, output_path, num_partitions=args.num_partitions)
    finally:
        engine.stop()

if __name__ == '__main__':
    main()
