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
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // map used to count in the files # appearences
        Map<String, Integer> docCounts = new HashMap<>();

        // iter on values
        for (Text val : values) {
            String[] docCountPairs = val.toString().split(",");

            for (String docCountPair : docCountPairs) {
                String[] parts = docCountPair.split(":");

                // bad-formatted stuff
                if (parts.length < 2) {
                    continue;
                }

                // it's possible that the doc-id has ':' character inside
                // the next part of code considers this fact and it gives the correct value to doc-id and count
                StringBuilder docIdBuilder = new StringBuilder();
                for (int i = 0; i < parts.length - 1; i++) {
                    if (i > 0) docIdBuilder.append(":");
                    docIdBuilder.append(parts[i]);
                }
                String docId = docIdBuilder.toString().trim();

                int count = Integer.parseInt(parts[parts.length - 1].trim());

                docCounts.put(docId, docCounts.getOrDefault(docId, 0) + count);
            }
        }

        // building output string
        StringBuilder outputValue = new StringBuilder();
        for (Map.Entry<String, Integer> entry : docCounts.entrySet()) {
            if (outputValue.length() > 0) {
                outputValue.append(", ");
            }
            String docOutput = entry.getKey() + ":" + entry.getValue();
            outputValue.append(docOutput);
        }

        // writing output in the correct format
        context.write(key, new Text(outputValue.toString()));
    }
}

