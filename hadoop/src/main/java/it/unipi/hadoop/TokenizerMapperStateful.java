package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.HashMap;
import java.util.Map;

import java.io.IOException;
    

// INPUT (key: offset, value: doc)
// OUTPUT (key: word, value: doc1:count, doc2:count, ...)

public class TokenizerMapperStateful extends Mapper<LongWritable, Text, Text, Text> {

// class used to implement internal combining
    public class WordOccurrences {
        private final String word;
        private final Map<String, Integer> docCounts;

        public WordOccurrences(String word) {
            this.word = word;
            this.docCounts = new HashMap<>();
        }

        public String getWord() {
            return word;
        }

        public void addOccurrence(String docId) {
            docCounts.put(docId, docCounts.getOrDefault(docId, 0) + 1);
        }

        public Map<String, Integer> getDocCounts() {
            return docCounts;
        }
    }


// Internal map structured as <word, [doc1, count1] ... [docN, countN]>
    private Map<String, WordOccurrences> wordCounts;
    private String filename;


// In the setup method, document name is retrieved
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        filename = fileSplit.getPath().getName();
        wordCounts = new HashMap<>();
    }

// The map method is very similiar to the standard word-count, with difference in the output format, since in this case document name is included.
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase().replaceAll("[.,:;]\'\"", "");
        String[] tokens = line.split("\\s+");
        for (String token : tokens) {
            if (!token.isEmpty()) {
                WordOccurrences occ = wordCounts.get(token);
                if (occ == null) {
                    occ = new WordOccurrences(token);
                    wordCounts.put(token, occ);
                }
                occ.addOccurrence(filename);
            }
        }
    }

// First solution --> The output is sent only one time, during the cleanup method in the form <word, doc1:count1 ... docN:countN>
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (WordOccurrences occ : wordCounts.values()) {
            StringBuilder valueBuilder = new StringBuilder();
            for (Map.Entry<String, Integer> entry : occ.getDocCounts().entrySet()) {
                if (valueBuilder.length() > 0) {
                    valueBuilder.append(", ");
                }
                valueBuilder.append(entry.getKey()).append(":").append(entry.getValue());
            }
            context.write(new Text(occ.getWord()), new Text(valueBuilder.toString()));
        }
    }

// Second solution --> The output is sent only one time, during the cleanup method in the form <word, doc1:count1 > ... <word, docN:countN>
/*
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (WordOccurrences occ : wordCounts.values()) {
            for (Map.Entry<String, Integer> entry : occ.getDocCounts().entrySet()) {
                context.write(new Text(occ.getWord()), new Text(entry.getKey() + ":" + entry.getValue()));
            }
        }
    }
*/
}