import argparse
import os
import re
import sys
import subprocess
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name

# Define HDFS paths
HDFS_BASE = 'hdfs:///user/hadoop/'
DATA_DIR = HDFS_BASE + 'inverted-index/data'
OUTPUT_BASE = HDFS_BASE + 'inverted-index/'

class InvertedIndexSearch:
    def __init__(self, app_name="InvertedIndexSearch"):
        conf = SparkConf().setAppName(app_name)
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.sc = self.spark.sparkContext

    _token_pattern = re.compile(r"[\W_]+")

    @staticmethod
    def tokenize(text):
        return InvertedIndexSearch._token_pattern.sub(" ", text.lower()).split()

    def build_index(self, input_paths, output_path, num_partitions=None):
        df = self.spark.read.text(input_paths)
        df = df.withColumn('filename', input_file_name())
        rdd = df.rdd.map(lambda r: (os.path.basename(r.filename), r.value))
        tokenize = InvertedIndexSearch.tokenize
        pairs = rdd.flatMap(lambda ft: [((w, ft[0]), 1) for w in tokenize(ft[1])])
        counts = pairs.reduceByKey(lambda a, b: a + b)
        word_file = counts.map(lambda wc: (wc[0][0], (wc[0][1], wc[1])))
        def seq_op(acc, fv): acc.setdefault(fv[0], 0); acc[fv[0]] += fv[1]; return acc
        def comb_op(a, b):
            for fn, c in b.items(): a.setdefault(fn, 0); a[fn] += c
            return a
        index = word_file.combineByKey(lambda fv: {fv[0]: fv[1]}, seq_op, comb_op)
        formatted = index.map(lambda kv: f"{kv[0]}\t" + ", ".join(f"{fn}:{cnt}" for fn, cnt in sorted(kv[1].items())))
        parts = num_partitions or self.sc.defaultParallelism
        formatted.repartition(parts).saveAsTextFile(output_path)

    def stop(self): self.sc.stop()

# HDFS utility functions
def hdfs_dir_exists(path):
    return subprocess.run(['hdfs', 'dfs', '-test', '-d', path]).returncode == 0


def list_hdfs_files(path):
    proc = subprocess.run(['hdfs', 'dfs', '-ls', path], capture_output=True, text=True, check=True)
    files = []
    for line in proc.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 8 and parts[0].startswith('-'):
            size = int(parts[4]); p = parts[7]
            files.append((p, size))
    return files


def choose_input_paths(limit_mb=None):
    if not hdfs_dir_exists(DATA_DIR):
        print("The hdfs://user/hadoop/inverted-index/data not exists. You have to generate it first.")
        return ""
    files = list_hdfs_files(DATA_DIR)
    if limit_mb is None:
        return [DATA_DIR + '/*']
    mb = limit_mb * 1024 * 1024
    selected, total = [], 0
    for p, sz in sorted(files, key=lambda x: x[1]):
        if total + sz > mb: break
        selected.append(p); total += sz
    return selected


def choose_output_path():
    base = OUTPUT_BASE + 'output'
    idx = ''
    while hdfs_dir_exists(base + idx):
        idx = str(int(idx or '0') + 1)
    return base + idx


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-partitions', type=int)
    known, unknown = parser.parse_known_args()
    # Parse limit flag --<N>MB
    limit = None
    for arg in unknown:
        m = re.match(r'--(\d+)MB$', arg)
        if m: limit = int(m.group(1)); break

    input_paths = choose_input_paths(limit)
    if(input_paths == ""):
        return
    output_path = choose_output_path()

    engine = InvertedIndexSearch()
    try:
        engine.build_index(input_paths, output_path, num_partitions=known.num_partitions)
        print(f"Index saved to {output_path}")
    finally:
        engine.stop()

if __name__ == '__main__': 
    main()
