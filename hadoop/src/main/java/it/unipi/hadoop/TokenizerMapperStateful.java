package it.unipi.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// INPUT (key: offset, value: doc)
// OUTPUT (key: word, value: doc1:count, doc2:count, ...)

public class TokenizerMapperStateful extends Mapper<LongWritable, Text, Text, Text> {

    private Map<String, Map<String, Integer>> wordCounts;
    private String filename;


// In the setup method, document name is retrieved
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        filename = new org.apache.hadoop.fs.Path(context.getConfiguration().get("map.input.file")).getName();
        wordCounts = new HashMap<>();
    }

// The map method is very similiar to the standard word-count, with difference in the output format, since in this case document name is included.
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString()
                .toLowerCase()
                .replaceAll("[^A-Za-z0-9\\s]", "") // rimuove punteggiatura e simboli
                .split("\\s+");

        for (String token : tokens) {
            if (!token.isEmpty()) {
                Map<String, Integer> docMap = wordCounts.computeIfAbsent(token, k -> new HashMap<>());
                docMap.put(filename, docMap.getOrDefault(filename, 0) + 1);
            }
        }
    }

// First solution --> The output is sent only one time, during the cleanup method in the form <word, doc1:count1 ... docN:countN>
/*
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
*/
// Second solution --> The output is sent only one time, during the cleanup method in the form <word, doc1:count1 > ... <word, docN:countN>
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Text wordText = new Text();
        Text valueText = new Text();

        for (Map.Entry<String, Map<String, Integer>> entry : wordCounts.entrySet()) {
            wordText.set(entry.getKey());
            for (Map.Entry<String, Integer> docEntry : entry.getValue().entrySet()) {
                valueText.set(docEntry.getKey() + ":" + docEntry.getValue());
                context.write(wordText, valueText);
            }
        }
    }
}