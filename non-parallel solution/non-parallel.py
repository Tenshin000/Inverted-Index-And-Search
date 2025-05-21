import os
import re
from collections import defaultdict
import time
import psutil  # For memory usage tracking
import sys

def build_inverted_index(input_dir):
    # Track execution time and memory
    start_time = time.time()
    process = psutil.Process()

    # Dictionary to store the inverted index: word -> {filename: count}
    inverted_index = defaultdict(lambda: defaultdict(int))

    # Iterate over all files in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith(".txt"):  # Process only .txt files
            filepath = os.path.join(input_dir, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                # Read the file content and tokenize into words
                content = f.read().lower()  # Case-insensitive
                words = re.findall(r'\b\w+\b', content)  # Extract words (alphanumeric)

                # Count word occurrences in this file
                for word in words:
                    inverted_index[word][filename] += 1

    # Print the inverted index to the console
    print("\nInverted Index:")
    print("--------------")
    for word in sorted(inverted_index.keys()):  # Sort words alphabetically
        file_counts = sorted(inverted_index[word].items())  # Sort files alphabetically
        file_list = ','.join(f"{filename}:{count}" for filename, count in file_counts)
        print(f"{word}\t{file_list}")

    # Calculate execution time and memory usage
    execution_time = time.time() - start_time
    memory_usage = process.memory_info().rss / 1024 / 1024  # Convert to MB

    return execution_time, memory_usage

def main():
    if len(sys.argv) != 2:
        print(f"Usage: python3 {os.path.basename(sys.argv[0])} <input_directory>")
        sys.exit(1)

    input_dir = sys.argv[1]
    if not os.path.isdir(input_dir):
        print(f"Error: Directory not found: {input_dir}")
        sys.exit(1)

    print(f"Building inverted index from {input_dir}...")
    exec_time, mem_usage = build_inverted_index(input_dir)
    print(f"\nExecution time: {exec_time:.2f} seconds")
    print(f"Memory usage: {mem_usage:.2f} MB")

if __name__ == "__main__":
    main()
