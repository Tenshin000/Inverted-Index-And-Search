package it.unipi.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;

public class TokenizerMapperStateful extends Mapper<LongWritable, Text, Text, Text> {
    // Threshold to trigger flushing of the in-memory word counts to context
    private static final int FLUSH_THRESHOLD = 100000;

    // Map to store word -> (document -> count) relationships
    private Map<String, Map<String, Integer>> wordCounts;

    private Text wordText = new Text();  // Hadoop output key (word)
    private Text valueText = new Text(); // Hadoop output value (filename:count)

    @Override
    protected void setup(Context context) {
        // Initialize the word count map
        wordCounts = new HashMap<>();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get full file path for the current split using a custom wrapper
        String fullFilePath = MyCombineFileRecordReaderWrapper.getCurrentFilePath();
        
        // Extract only the filename from the full file path
        String currentFilename = "unknown";
        if (fullFilePath != null) {
            Path path = new Path(fullFilePath);
            currentFilename = path.getName();  // Just the file name
        }

        // Fallback to "unknown" if filename extraction failed
        if (currentFilename == null) {
            currentFilename = "unknown";
        }

        // Clean and tokenize the line: remove non-alphanumerics and split by whitespace
        String[] tokens = value.toString()
                .toLowerCase()
                .replaceAll("[^\\p{L}0-9\\s]", " ")  // Replace punctuation with space
                .split("\\s+");  // Split on one or more whitespace characters

        // Count each token per document
        for (String token : tokens) {
            if (token.isEmpty()) 
                continue;

            // Get or create the map for this word
            Map<String, Integer> docMap = wordCounts.computeIfAbsent(token, k -> new HashMap<>());
            // Increment the count for this filename
            docMap.put(currentFilename, docMap.getOrDefault(currentFilename, 0) + 1);
        }

        // Flush intermediate results if the threshold is reached
        if (wordCounts.size() >= FLUSH_THRESHOLD) {
            flush(context);
        }
    }

    // Write the current word counts to the context and clear the in-memory map
    private void flush(Context context) throws IOException, InterruptedException {
        for (Map.Entry<String, Map<String, Integer>> wordEntry : wordCounts.entrySet()) {
            String word = wordEntry.getKey();
            Map<String, Integer> docMap = wordEntry.getValue();

            for (Map.Entry<String, Integer> docEntry : docMap.entrySet()) {
                // Set output key as the word
                wordText.set(word);
                // Set output value as "filename:count"
                valueText.set(docEntry.getKey() + ":" + docEntry.getValue());
                // Emit the key-value pair
                context.write(wordText, valueText);
            }
        }
        // Clear memory after flush
        wordCounts.clear();
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Flush any remaining data in memory at the end of the task
        flush(context);
    }
}
