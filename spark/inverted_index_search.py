import argparse
import os
import sys
import re
from pyspark import SparkConf, SparkContext

class InvertedIndexSearch:
    def __init__(self, app_name="InvertedIndexSearch"):
        # Initialize SparkContext
        conf = SparkConf().setAppName(app_name)
        # Uncomment to run locally
        # conf = conf.setMaster("local[*]")
        self.sc = SparkContext(conf=conf)

    @staticmethod
    def _tokenize(text):
        """
        Simple tokenizer: convert to lowercase, remove punctuation, split on whitespace.
        """
        cleaned = re.sub(r"[\W_]+", " ", text.lower())
        return cleaned.split()

    def build_index(self, input_path, output_path):
        """
        Build inverted index from files at input_path and save to output_path.
        Output format: word\tfile1:count1, file2:count2, ...
        """
        # Reference tokenize function locally to avoid capturing self
        tokenize = InvertedIndexSearch._tokenize

        # Read all files as (path, content)
        files_rdd = self.sc.wholeTextFiles(input_path)

        # Create ((word, filename), 1) pairs
        pairs = files_rdd.flatMap(lambda fc: [((word, os.path.basename(fc[0])), 1)
                                              for word in tokenize(fc[1])])

        # Sum counts per (word, filename)
        counts = pairs.reduceByKey(lambda x, y: x + y)

        # Map to (word, (filename, count))
        word_file_counts = counts.map(lambda wc: (wc[0][0], (wc[0][1], wc[1])))

        # Group by word
        grouped = word_file_counts.groupByKey()

        # Format each line
        formatted = grouped.map(lambda wc: f"{wc[0]}\t" + '\t'.join(f"{fn}:{cnt}" for fn, cnt in sorted(wc[1])))

        # Save index
        formatted.saveAsTextFile(output_path)
        print(f"Index saved to {output_path}")

    def load_index(self, index_path):
        """
        Load inverted index from HDFS into a local dict: { word: [filenames] }
        """
        raw = self.sc.textFile(index_path)
        # Split lines into (word, file_list)
        pairs = raw.map(lambda line: line.split('\t', 1)).filter(lambda parts: len(parts) == 2)
        # Extract filenames
        word_to_files = pairs.map(lambda wf: (wf[0], [fc.split(':')[0] for fc in wf[1].split(', ')]))
        self.index = dict(word_to_files.collect())

    def query(self, terms):
        """
        Return sorted list of filenames containing all query terms.
        """
        if not hasattr(self, 'index'):
            raise ValueError("Index not loaded. Call load_index() first.")

        # Lowercase terms
        term_sets = [set(self.index.get(t.lower(), [])) for t in terms]
        if not term_sets:
            return []

        # Intersect all sets
        result = set.intersection(*term_sets)
        return sorted(result)


def main():
    parser = argparse.ArgumentParser(description="Inverted Index with Spark and HDFS support")
    parser.add_argument('inputs', nargs='+', help='Input file(s) or directory names (relative)')
    parser.add_argument('output', help='Output directory name (relative)')
    args = parser.parse_args()

    if len(args.inputs) < 1:
        parser.error('At least one input must be specified')

    # HDFS absolute prefix
    hdfs_base = 'hdfs://hadoop-namenode:9820/user/hadoop/'

    # Build full HDFS paths
    input_paths = [hdfs_base + inp.strip('/') for inp in args.inputs]
    input_str = ','.join(input_paths)
    output_path = hdfs_base + args.output.strip('/')

    # Run index build
    engine = InvertedIndexSearch()
    try:
        engine.build_index(input_str, output_path)
    finally:
        engine.sc.stop()


if __name__ == '__main__':
    main()
