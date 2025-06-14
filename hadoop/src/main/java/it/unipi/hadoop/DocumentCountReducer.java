package it.unipi.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// INPUT  (key: word, value: doc:count)
// OUTPUT (key: word, value: doc1:countSum,doc2:countSum,...)
public class DocumentCountReducer extends Reducer<Text, Text, Text, Text> {

    // Reusable StringBuilder for output string construction
    private final StringBuilder outputBuilder = new StringBuilder();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // Map to store the sum of counts per document ID
        Map<String, Integer> docCounts = new HashMap<>();

        for (Text val : values) {
            String s = val.toString();

            // Find the last ':' which separates filename and count
            int idx = s.lastIndexOf(':');
            if (idx <= 0 || idx == s.length() - 1) {
                // Skip malformed entries
                continue;
            }

            // Extract document ID (filename) and count
            String docId = s.substring(0, idx);
            String countStr = s.substring(idx + 1);

            try {
                int count = Integer.parseInt(countStr);
                // Merge count into the map
                docCounts.merge(docId, count, Integer::sum);
            } catch (NumberFormatException e) {
                // Skip invalid count formats
            }
        }

        // Build the output value \t doc1:sum1 \t doc2:sum2,...
        outputBuilder.setLength(0); // clear previous content
        for (Map.Entry<String, Integer> entry : docCounts.entrySet()) {
            if (outputBuilder.length() > 0) {
                outputBuilder.append("\t");
            }
            outputBuilder
                .append(entry.getKey())
                .append(':')
                .append(entry.getValue());
        }

        // Emit the final output key-value pair
        context.write(key, new Text(outputBuilder.toString()));
    }
}
