package it.unipi.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// INPUT (key: word, value: doc1:count, doc2:count, ...)
// OUTPUT (key: word, value: doc1:countSum, doc2:countSum, ...)

public class DocumentCountReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // map used to count in the files # appearances
        Map<String, Integer> docCounts = new HashMap<>();
        
        // iter over values
        for (Text value : values) {
            for (String pair : value.toString().split(",")) {
                String[] parts = pair.trim().split(":");
                if (parts.length == 2) {
                    // skip malformed input
                    try {
                        // skip invalid count
                        docCounts.merge(parts[0], Integer.parseInt(parts[1]), Integer::sum);
                    } catch (NumberFormatException ignored) {}
                }
            }
        }
        
        // building output string efficiently
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, Integer> entry : docCounts.entrySet()) {
            if (result.length() > 0) result.append("\t");
            result.append(entry.getKey()).append(":").append(entry.getValue());
        }

        // writing output in the correct format
        context.write(key, new Text(result.toString()));
    }
}
