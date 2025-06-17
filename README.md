# Inverted Index and Search
Cloud Computing project, a.y. 2024/2025

Univeristy of Pisa,
Department of Information Engineering, 
m.sc. Computer Engineering / m.sc. Artificial Intelligence and Data Engineering


Francesco Panattoni,
Lorenzo Vezzani,
Hajar Makhlouf

## Introduction  
Our project for the Cloud Computing course involves developing a basic search engine backend through the construction of an **Inverted Index**, a fundamental data structure in information retrieval systems such as **Google Search**. The main goal is to process a collection of text files and efficiently map each word to the files in which it appears, along with the frequency of its occurrences. Subsequently we had to analyze and compare the performance of a Java-based application using the **Hadoop** framework with that of a Python-based application using the **Spark** framework. Finally we have to build a **search query** in Python based on the inverted indexes produced.

We tried to make the code as optimized as possible by doing a lot of tests to try to optimize the execution time and memory usage.

## Equipment
The tests were conducted on three identical virtual machines, each configured with the following hardware and software specifications:

- **CPU**: 2 virtual CPUs (vCPUs), mapped to Intel(R) Xeon(R) Silver 4208 CPU @2.10GHz, provided via KVM virtualization
- **RAM**: 6.8 GB of system memory
- **Disk**: 40 GB of allocated virtual storage (ext4 filesystem)
- **Operating System**: Ubuntu 22.04.1 LTS (Jammy Jellyfish), 64-bit

## Dataset
We selected a 1583.5 MB corpus of 2685 plaintext files from [Project Gutenberg](https://www.gutenberg.org/), covering diverse fields including philosophy, science, theology, psychology, literature and other cultural subjects, to stress and tests our indexer across a broad range of real-world texts. This variety tests the system against typical literary content as well as challenging patterns, mirroring real-world search engine demands on both natural language and specialized data. File sizes vary from 5 KB to 250 MB: most are under 1 MB (reflecting typical book chapters or short essays), 329 fall between 1 MB and 7 MB (full-length books) and one extreme outlier ("Human_Genome_Project-Chromosome_1.txt", 250 MB) contains raw nucleotide sequences. Including this genomic text deliberately exposes our inverted-index builder to vast, mostly unique tokens-mimicking workloads with high vocabulary cardinality and ensuring our system handles both common-word skew and near-unique string distributions. By including files ranging from kilobytes to megabytes, the dataset enables a rigorous evaluation of how indexing and search-query systems scale with input size.

## MapReduce and Hadoop code
The system uses **MapReduce**, via the **Hadoop** framework, to process large-scale data efficiently. The Hadoop cluster is optimized for virtual machines with limited memory through customized YARN and MapReduce settings. YARN manages resources and memory (up to 5 GB per node), while MapReduce configurations allocate 2048 MB to key tasks, with JVM heaps limited ot 1536 MB.

``` PseudoCode
class TokenizerMapperStateful

	method initialize()
		word_counts <= New Empty AssociativeArray()
	end method
	
	method map(offset o, doc d)
		Filename <= retrieve_file_name()
		
		for all term t in doc d do
			if word_counts[t] does not contain Filename then
				word_counts[t][Filename] <= 1
			else
				word_counts[t][Filename] <= word_counts[t][Filename] + 1
			end if
		end for
		
		if word_counts.size() > FLUSH_THRESHOLD then
			flush(context)
		end if
	end method
	
	method flush(context)
		for each word in word_counts do
			for each filename in word_counts[word] do
				value <= filename + ":"+ 
				+word_counts[word][filename]
				
				emit(word, value)
			end for
		end for
		clear word_counts
	end method
	
	method cleanup(context)
		flush(context)
	end method

end class
		
```

<span id="fig:pseudocode-stateful-mapper" label="fig:pseudocode-stateful-mapper"></span>

``` PseudoCode
class TokenizerMapper
	method map(offset o, doc d)
		Filename <= retrieve_file_name()
		for all term t in doc d do
			emit(term t, filename + ":1")
		end for
	end method
end class
		
```

<span id="fig:pseudocode-stateless-mapper" label="fig:pseudocode-stateless-mapper"></span>

``` PseudoCode
class DocumentCountReducer
	method reduce(term, postingsList)
		docCounts <= {}
		for all posting in postingsList do
			for pair in split(posting, ",") do
				doc, cnt <= splitLast(pair, ":")
				docCounts[doc] docCounts.get(doc, 0) + toInt(cnt)
			endfor
		endfor
	emit(term, format(docCounts))
	end method
end class
			
```

<span id="fig:pseudocode-reducer" label="fig:pseudocode-reducer"></span>

The application offers two interchangeable MapReduce variants: a classic **Mapper with optional Combiner** and a **Stateful In-Mapper Combiner**, selectable via command-line flags for modularity and flexibility.

To address Hadoop’s inefficiency with many small files, we use `CombineFileSplit` to reduce task overhead.

In Hadoop, the `CombineFileInputFormat` class does not know by itself how to read each individual file within a `CombineFileSplit`. For this reason, it requires a **custom RecordReader** for each combined file. This is the job of `MyCombineTextInputFormat`.

`MyCombineFileRecordReaderWrapper` is a wrapper around LineRecordReader, which reads one line at a time as in a normal Hadoop job. Its main function, however, is another: **it keeps track of the name of the file it is reading from**, using a `ThreadLocal` object. This is essential for an inverted index, because each word read from the line must be associated with the document (i.e. the file) in which it appears.

The `TokenizerMapStateful` class accumulates word counts in memory using a data structure initialized in `setup()`, mapping words to document-specific counts. Once a predefined **threshold** is exceeded, it triggers a **flush**, emitting partial results in the format: ⟨**word**, **doc-id:count**⟩

Residual data is emitted during `cleanup()`.

In contrast, the `TokenizerMapper` class—lacking in-mapper combining—directly emits key-value pairs of the form:
⟨**word**, **doc-id:1**⟩

The `CombinerDocCounts` class implements the Combiner logic, aggregating intermediate values by summing occurrences per document:
⟨**word**, **doc-id:count**⟩

Finally, the `DocumentCountReducer` consolidates all partial counts per word across files and formats the output as:

`word \t filename1:count1 \t filename2:count2`

