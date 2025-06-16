import argparse
import heapq
import os
import psutil
import re
import shutil
import sys
import tempfile
import time

from collections import defaultdict
from hdfs import InsecureClient
from urllib.parse import urlparse

# HDFS configuration constants
HDFS_URL = 'http://10.1.1.218:9870'
HDFS_USER = 'hadoop'

# Default URIs for input and output
DEFAULT_INPUT = 'hdfs:///user/hadoop/inverted-index/data'
DEFAULT_OUTPUT = 'hdfs:///user/hadoop/inverted-index/output'
# Number of files to process in-memory before flushing to a block\BLOCK_SIZE = 200


def parse_args():
    """
    Parse command-line arguments for input/output paths and optional size limit.
    """
    parser = argparse.ArgumentParser(
        description='Build an inverted index from local or HDFS text files using SPIMI-style block writing.')
    parser.add_argument(
        '--input', '-i', default=DEFAULT_INPUT,
        help='Input directory URI (local path or hdfs://...)')
    parser.add_argument(
        '--output', '-o', default=DEFAULT_OUTPUT,
        help='Output base URI (local path or hdfs://...)')
    parser.add_argument(
        '--limit-mb', '-l', type=int, default=None,
        help='Optional size limit in MB for total data processed')
    return parser.parse_args()


def next_available_dir(base_path, is_hdfs=False, hdfs_client=None):
    """
    Determine the next directory name for non-parallel output to avoid overwriting existing data.
    If HDFS is used, list via client; else list local filesystem.
    """
    existing = []
    if is_hdfs:
        try:
            existing = hdfs_client.list(base_path)
        except Exception:
            existing = []
    else:
        if os.path.isdir(base_path):
            existing = os.listdir(base_path)

    # Find highest numeric suffix in directories named 'output-non-parallelX'
    max_n = -1
    for name in existing:
        if name.startswith('output-non-parallel'):
            suffix = name.replace('output-non-parallel', '')
            if suffix.isdigit():
                max_n = max(max_n, int(suffix))
    next_n = max_n + 1
    return os.path.join(base_path, f"output-non-parallel{next_n}")


def spimi_block_write(inverted_index, block_dir, block_id):
    """
    Write the in-memory inverted index to a block file, sorted by term.
    Returns the path to the written block. Single Pass In Memory Indexing. 
    """
    os.makedirs(block_dir, exist_ok=True)
    block_path = os.path.join(block_dir, f"block_{block_id}.txt")
    with open(block_path, 'w', encoding='utf-8') as bf:
        # Write each term along with its postings list
        for word in sorted(inverted_index):
            postings = inverted_index[word]
            line = '\t'.join(f"{doc}:{cnt}" for doc, cnt in sorted(postings.items()))
            bf.write(f"{word}\t{line}\n")
    print("Flush...")  # Log flush event
    return block_path


def build_blocks(input_path, hdfs_client=None, max_bytes=None):
    """
    Read files from the input path, build partial inverted index blocks,
    flush to disk every BLOCK_SIZE files or when size limit reached.
    Returns list of block file paths, the temp directory, elapsed time, and memory used.
    """
    print("Starting...\n\n")
    start_time = time.time()
    process = psutil.Process()

    # Prepare list of filenames from HDFS or local
    if hdfs_client:
        filenames = sorted(hdfs_client.list(input_path))
    else:
        filenames = sorted(
            f for f in os.listdir(input_path)
            if os.path.isfile(os.path.join(input_path, f)))

    total = 0
    block_id = 0
    file_count = 0
    inverted_index = defaultdict(lambda: defaultdict(int))
    # Create a temporary directory to store block files
    block_dir = tempfile.mkdtemp(prefix='spimi_blocks_')
    block_files = []

    for fname in filenames:
        # Process only .txt files
        if not fname.endswith('.txt'):
            continue
        full_path = os.path.join(input_path, fname)
        # Get file size
        if hdfs_client:
            status = hdfs_client.status(full_path)
            size = status.get('length', 0)
        else:
            size = os.path.getsize(full_path)

        print(f"Reading file: {fname} ({size/1024:.2f} KB)")

        # Enforce size limit if specified
        if max_bytes is not None and total + size > max_bytes:
            break
        total += size

        # Read file content, decode if from HDFS
        try:
            if hdfs_client:
                with hdfs_client.read(full_path) as reader:
                    raw = reader.read()
                    text = raw.decode('utf-8', errors='ignore').lower()
            else:
                with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                    text = f.read().lower()
        except Exception:
            continue  # Skip unreadable files

        # Normalize text: replace punctuation/underscores with spaces
        normalized = re.sub(r"[^\w\s]|_", " ", text)
        del text

        # Tokenize and update postings in-memory
        for w in re.findall(r'\b\w+\b', normalized):
            inverted_index[w][fname] += 1

        file_count += 1
        del normalized

        # Flush to disk if block size reached
        if file_count >= BLOCK_SIZE:
            path = spimi_block_write(inverted_index, block_dir, block_id)
            block_files.append(path)
            inverted_index.clear()
            file_count = 0
            block_id += 1

    # Flush any remaining data
    if inverted_index:
        path = spimi_block_write(inverted_index, block_dir, block_id)
        block_files.append(path)

    elapsed = time.time() - start_time
    memory_mb = process.memory_info().rss / 1024 / 1024
    print(f"Blocks built in {elapsed:.2f}s, mem {memory_mb:.2f}MB")
    return block_files, block_dir, elapsed, memory_mb


def merge_blocks(block_files, output_uri, hdfs_client=None):
    """
    Merge sorted block files into a single inverted index output.
    Uses a heap to efficiently merge postings lists by term.
    Writes result to local or HDFS output directory.
    Returns the path to the merged output file.
    """
    parsed = urlparse(output_uri)
    is_hdfs = parsed.scheme == 'hdfs'
    base = parsed.path if is_hdfs else output_uri

    # Prepare output directory in HDFS or local
    if is_hdfs:
        client = hdfs_client or InsecureClient(HDFS_URL, user=HDFS_USER)
        client.makedirs(base)
        out_dir = next_available_dir(base, is_hdfs=True, hdfs_client=client)
        client.makedirs(out_dir)
        out_path = os.path.join(out_dir, 'output.txt')
        writer_ctx = client.write(out_path, encoding='utf-8')
    else:
        os.makedirs(base, exist_ok=True)
        out_dir = next_available_dir(base)
        os.makedirs(out_dir)
        out_path = os.path.join(out_dir, 'output.txt')
        writer_ctx = open(out_path, 'w', encoding='utf-8')

    with writer_ctx as writer:
        # Write header
        writer.write('Inverted Index:\n')
        writer.write('--------------\n')

        # Open all block files
        files = [open(f, 'r', encoding='utf-8') for f in block_files]
        heap = []
        # Initialize heap with first line from each block
        for idx, f in enumerate(files):
            line = f.readline()
            if line:
                word, rest = line.strip().split('\t', 1)
                heap.append((word, idx, rest))
        heapq.heapify(heap)

        current_word = None
        current_postings = []
        # Merge loop: extract lowest term and combine postings
        while heap:
            word, idx, rest = heapq.heappop(heap)
            postings = rest.split('\t')
            if word != current_word:
                # Write accumulated postings for previous term
                if current_word is not None:
                    writer.write(f"{current_word}\t" + '\t'.join(current_postings) + "\n")
                current_word = word
                current_postings = postings
            else:
                # Append postings for same term
                current_postings.extend(postings)
            # Read next line from the same file and push to heap
            line = files[idx].readline()
            if line:
                w, r = line.strip().split('\t', 1)
                heapq.heappush(heap, (w, idx, r))

        # Write last term postings
        if current_word is not None:
            writer.write(f"{current_word}\t" + '\t'.join(current_postings) + "\n")

        # Close all file handles
        for f in files:
            f.close()

    return out_path


def main():
    # Parse CLI arguments
    args = parse_args()
    input_uri = args.input
    output_uri = args.output
    limit_bytes = args.limit_mb * 1024 * 1024 if args.limit_mb else None

    # Determine if input is HDFS or local, set client and path
    parsed_input = urlparse(input_uri)
    if parsed_input.scheme == 'hdfs':
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        input_path = parsed_input.path
    else:
        client = None
        input_path = input_uri
        if not os.path.isdir(input_path):
            print(f"Error: input directory not found: {input_path}")
            sys.exit(1)

    print("Non-Parallel code to build an Inverted-Index ... \n \n")

    print(f"Reading from {'HDFS' if client else 'local'}: {input_uri}")
    total_start = time.time()  # Track total execution time

    # Build SPIMI blocks
    block_files, block_dir, elapsed, mem = build_blocks(input_path, client, limit_bytes)
    print(f"Merging {len(block_files)} blocks...")

    # Merge blocks into final output
    merge_start = time.time()
    out_path = merge_blocks(block_files, output_uri, client)
    merge_time = time.time() - merge_start

    total_time = time.time() - total_start

    # Print summary statistics
    print(f"Output written to: {out_path}")
    print(f"\n===== Execution Summary =====")
    print(f"Block construction time: {elapsed:.2f} s")
    print(f"Merge (write) time: {merge_time:.2f} s")
    print(f"Execution Time: {total_time:.2f} s")
    print(f"Peak memory used: {mem:.2f} MB")
    print("=============================")

    # Clean up temporary block directory
    shutil.rmtree(block_dir)


if __name__ == '__main__':
    main()
