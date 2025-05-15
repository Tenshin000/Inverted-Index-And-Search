import sys
import os
import glob

def search_inverted_index(query, index_dir):
    query_terms = set(term.lower() for term in query.split())
    if not query_terms:
        return ["Error: Empty query."]

    term_to_files = {term: set() for term in query_terms}
    for filepath in glob.glob(os.path.join(index_dir, "part-*")):
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith("("):  # Spark format
                    word, file_list = eval(line)
                else:  # Hadoop format
                    word, files = line.split("\t")
                    file_list = files.split(",")

                word = word.lower()
                if word in query_terms:
                    for file_count in file_list:
                        filename = file_count.split(":")[0]
                        term_to_files[word].add(filename)

    result_files = term_to_files[list(query_terms)[0]]
    for term in query_terms:
        result_files = result_files.intersection(term_to_files[term])

    return sorted(list(result_files)) if result_files else ["No matches found."]

def main():
    if len(sys.argv) != 2:
        print(f"Usage: python3 {os.path.basename(sys.argv[0])} <index_directory>")
        sys.exit(1)

    index_dir = sys.argv[1]
    if not os.path.isdir(index_dir):
        print(f"Error: Directory not found: {index_dir}")
        sys.exit(1)

    query = input("Select a query: ").strip()
    if not query:
        print("Error: Empty query.")
        sys.exit(1)

    results = search_inverted_index(query, index_dir)
    for result in results:
        print(result)

if __name__ == "__main__":
    main()
