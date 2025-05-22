import sys 
import os
import glob

# The function takes the query, splits it into words, and convert them to lower case for case insensitivity
def search_inverted_index(query, index_dir):
    query_terms = set(term.lower() for term in query.split())
    if not query_terms:
        return ["Error: Empty query."]

    term_to_files = {term: set() for term in query_terms} # it creates a terms associated with its filesname (empty set)
    for filepath in glob.glob(os.path.join(index_dir, "part-*")): # This assumes the inverted index is split across multiple files (common in Hadoop or Spark).
        with open(filepath, 'r', encoding='utf-8') as f: 
            for line in f:
                line = line.strip()  #removes any whitespaces or newlines from the line
                if line.startswith("("):  # Spark format
                    word, file_list = eval(line)  #parse the word and its filename into one unit
                else:  # Hadoop format
                    word, files = line.split("\t")
                    file_list = files.split(",")      

                word = word.lower()
                if word in query_terms:   # taking only the filename without its counting file
                    for file_count in file_list:
                        filename = file_count.split(":")[0]
                        term_to_files[word].add(filename)

    result_files = term_to_files[list(query_terms)[0]] #This serves as the starting point for finding files that contain all query terms
    for term in query_terms: # filtering
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
