import argparse
import os
import sys
from pyspark import SparkConf, SparkContext

class InvertedIndexSearch:
    def __init__(self, app_name="InvertedIndexSearch"):
        conf = SparkConf().setAppName(app_name)
        # If running locally for testing, uncomment next line:
        # conf = conf.setMaster("local[*]")
        self.sc = SparkContext(conf=conf)
        self.index_rdd = None

    def build_index(self, input_path, output_path):
        """
        Build inverted index from files at input_path, save to output_path.
        Uses wholeTextFiles to capture filename with content.
        Output format: word \t file1:count1, file2:count2, ...
        """
        # Read all files as (path, content)
        files_rdd = self.sc.wholeTextFiles(input_path)

        # Extract (word, filename) pairs
        pairs = files_rdd.flatMap(lambda fc: [((word, fc[0].split('/')[-1]), 1)
                                              for word in self._tokenize(fc[1])])

        # Count occurrences per (word, filename)
        counts = pairs.reduceByKey(lambda x, y: x + y)

        # Transform to (word, (filename, count))
        word_file_counts = counts.map(lambda wc: (wc[0][0], (wc[0][1], wc[1])))

        # Group by word
        grouped = word_file_counts.groupByKey()

        # Format output strings
        formatted = grouped.map(lambda wc: (wc[0], ", ".join([f"{fn}:{cnt}" for fn, cnt in sorted(wc[1])])))

        # Save as text file
        formatted.map(lambda wc: f"{wc[0]}\t{wc[1]}").saveAsTextFile(output_path)
        print(f"Index saved to {output_path}")

    def load_index(self, index_path):
        """
        Load inverted index from saved files into an in-memory dict: { word: set(filenames) }
        """
        raw = self.sc.textFile(index_path)
        # Each line: word\tfile1:cnt1, file2:cnt2,...
        pairs = raw.map(lambda line: line.split('\t', 1)).filter(lambda parts: len(parts) == 2)
        word_to_files = pairs.map(lambda wf: (wf[0], [fc.split(':')[0] for fc in wf[1].split(', ')]))
        self.index_rdd = dict(word_to_files.collect())

    def query(self, terms):
        """
        Query the in-memory index for terms (list of words).
        Returns list of filenames containing all terms.
        """
        if self.index_rdd is None:
            raise ValueError("Index not loaded. Call load_index() first.")

        # Collect sets per term
        sets = []
        for term in terms:
            files = set(self.index_rdd.get(term.lower(), []))
            sets.append(files)

        if not sets:
            return []
        # Intersect sets
        result = set.intersection(*sets)
        return sorted(result)

    @staticmethod
    def _tokenize(text):
        # Simple tokenizer: lowercase, remove punctuation, split on whitespace
        import re
        cleaned = re.sub(r"[\W_]+", " ", text.lower())
        return cleaned.split()


def main():
    parser = argparse.ArgumentParser(description="Inverted Index and Search with Spark")
    subparsers = parser.add_subparsers(dest='command')

    # Build index command
    build_parser = subparsers.add_parser('index', help='Build the inverted index')
    build_parser.add_argument('input_path', help='Input directory or file path')
    build_parser.add_argument('output_path', help='Output directory for index')

    # Query command
    query_parser = subparsers.add_parser('search', help='Search the inverted index')
    query_parser.add_argument('index_path', help='Path to saved index files (directory)')
    query_parser.add_argument('terms', nargs='+', help='Search terms')

    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    args.input_path  = os.path.normpath(os.path.join(base_dir,  args.input_path))
    args.output_path = os.path.normpath(os.path.join(base_dir,  args.output_path))

    if args.command == 'index':
        engine = InvertedIndexSearch()
        engine.build_index(args.input_path, args.output_path)
    elif args.command == 'search':
        engine = InvertedIndexSearch()
        engine.load_index(args.index_path)
        results = engine.query(args.terms)
        for filename in results:
            print(filename)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
