import sys
import os
import glob
import argparse
from hdfs import InsecureClient

HDFS_URL = 'http://10.1.1.218:9870'
HDFS_USER = 'hadoop'


def read_hdfs_files(hdfs_path, hdfs_url=HDFS_URL, user=HDFS_USER):
    client = InsecureClient(hdfs_url, user=user)
    files = client.list(hdfs_path)
    contents = []

    for file in files:
        file_path = os.path.join(hdfs_path, file)
        with client.read(file_path) as reader:
            for line in reader:
                contents.append(line.decode('utf-8').strip())

    return contents

def read_local_files(index_dir):
    contents = []
    for filepath in glob.glob(os.path.join(index_dir, "*")):
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                contents.append(line.strip())
    return contents

def build_term_offset_index_from_lines(lines):
    term_index = {}
    offset = 0

    for line in lines:
        if not line or line.startswith("Inverted Index") or line.startswith("----"):
            offset += 1
            continue

        parts = line.split("\t")
        if len(parts) < 2:
            offset += 1
            continue

        word = parts[0].lower()
        if word not in term_index:
            term_index[word] = offset
        offset += 1

    return term_index

def search_from_lines(query, lines, term_index):
    query_terms = set(term.lower() for term in query.split())
    if not query_terms:
        return ["Error: Empty query."]

    term_to_files = {}

    for term in query_terms:
        if term not in term_index:
            return ["No matches found."]
        line = lines[term_index[term]]
        parts = line.split("\t")
        file_list = parts[1:] if len(parts) >= 2 else []
        filenames = set(fc.split(":")[0] for fc in file_list)
        term_to_files[term] = filenames

    result_files = term_to_files[list(query_terms)[0]]
    for term in query_terms:
        result_files = result_files.intersection(term_to_files[term])

    return sorted(result_files) if result_files else ["No matches found."]

def get_index_data(args):
    if args.folder:
        lines = read_local_files(args.folder)
        return lines
    elif args.hadoop:
        return read_hdfs_files("/user/hadoop/inverted-index/search/output-hadoop")
    elif args.spark:
        return read_hdfs_files("/user/hadoop/inverted-index/search/output-spark-df")
    elif args.spark_df:
        return read_hdfs_files("/user/hadoop/inverted-index/search/output-spark-df")
    elif args.spark_rdd:
        return read_hdfs_files("/user/hadoop/inverted-index/search/output-spark-rdd")
    elif args.non_parallel:
        return read_hdfs_files("/user/hadoop/inverted-index/search/output-non-parallel")
    else:
        return []

def main():
    parser = argparse.ArgumentParser(description="Inverted Index Search Tool")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--folder", type=str, help="Specify a local folder with the inverted index files")
    group.add_argument("--hadoop", action="store_true", help="Use Hadoop output directory")
    group.add_argument("--spark", action="store_true", help="Use Spark (DataFrame) output directory")
    group.add_argument("--spark-df", action="store_true", help="Use Spark DataFrame output directory")
    group.add_argument("--spark-rdd", action="store_true", help="Use Spark RDD output directory")
    group.add_argument("--non-parallel", action="store_true", help="Use the Non-Parallel code output directory")

    args = parser.parse_args()
    
    print("*" * 40)
    print("*" + " " * 38 + "*")
    print("*" + "Inverted Index Search Tool".center(38) + "*")
    print("*" + " " * 38 + "*")
    print("*" * 40)
    print("\n")

    print("Loading index data...")
    lines = get_index_data(args)
    if not lines:
        print("Error: Could not load any index data.")
        sys.exit(1)

    term_index = build_term_offset_index_from_lines(lines)
    print("Index loaded. Enter queries (Ctrl+D to exit):")

    try:
        while True:
            query = input("> ").strip()
            if not query:
                continue
            results = search_from_lines(query, lines, term_index)
            print("=== Search results: ===\n")
            for res in results:
                print(res)
            print("\n")
    except EOFError:
        print("\nBye :)")

if __name__ == "__main__":
    main()
