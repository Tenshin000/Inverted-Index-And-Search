package it.unipi.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// INPUT  (key: word, value: doc1:count,doc2:count,...)
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
            int start = 0;
            int len = s.length();

            // Iterate through comma-separated pairs without splitting the whole string
            while (start < len) {
                int comma = s.indexOf(',', start);
                int end = (comma >= 0 ? comma : len);

                // Extract the substring for the current pair
                String pair = s.substring(start, end).trim();
                start = end + 1;

                // Find the last ':' to handle doc IDs that may contain ':'
                int colon = pair.lastIndexOf(':');
                if (colon <= 0 || colon == pair.length() - 1) {
                    continue; // malformed pair, skip it
                }

                String docId = pair.substring(0, colon);
                String countStr = pair.substring(colon + 1);
                try {
                    int count = Integer.parseInt(countStr);
                    // Merge count into the map
                    docCounts.merge(docId, count, Integer::sum);
                } catch (NumberFormatException e) {
                    // Skip invalid count formats
                }
            }
        }

        // Build the output value: doc1:sum1,doc2:sum2,...
        outputBuilder.setLength(0); // clear previous content
        for (Map.Entry<String, Integer> entry : docCounts.entrySet()) {
            if (outputBuilder.length() > 0) {
                outputBuilder.append(",");
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