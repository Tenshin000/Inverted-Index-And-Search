"""
Implement a very simple search-query system as a non-parallel Python script, which returns all the filenames containing the query term. 
The output must contain only the filenames, without any indication of the number of occurrences in each file. 
If the query is composed of multiple terms, the system must return the files that contain all the terms in the search query.

E.g., assume the search query is "cloud computing". doc1 only contains "cloud", whereas doc2 contains both "cloud" and "computing" (even not close to one another).
In this example, the search output is only doc2.txt
"""

import sys
import os

# utility class for Word
class Word:
    def __init__(self, value):
        self.value = value.lower()
        self.found = False

def main():
    if len(sys.argv) <= 1:
        print("Error: specify least one text file")
        print(f"Example: python3 {os.path.basename(sys.argv[0])} file1.txt [file2.txt ...]")
        sys.exit(1)

    query = input("Select a query: ")

    # search in the files
    for filename in sys.argv[1:]:
        if not os.path.isfile(filename):
            print(f"File not found: {filename}")
            continue

        # read the file
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read().lower()

        words = [Word(w) for w in query.split()]
        count = 0

        # search the words which compose the query in the file
        for word in words:
            if word.value in content and word.found == False:
                word.found = True
                count += 1

        # print file name if all words are found
        if(count == len(words)):
            print(filename)

if __name__ == "__main__":
    main()
