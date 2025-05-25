package it.unipi.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

    
// INPUT (key: word, docK:1)
// OUTPUT (key: word, value: doc1:countSum)
// OUTPUT (key: word, value: doc2:countSum)
// ...
// OUTPUT (key: word, value: docN:countSum)

public class CombinerDocCounts extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // map used to count in the files # appearences
        // structure: <doc-id, count>
        Map<String, Integer> docCounts = new HashMap<>();

        // iter on values
        for (Text val : values) {
            String[] parts = val.toString().split(":");
            String docId = parts[0];
            int count = Integer.parseInt(parts[1]);

            docCounts.put(docId, docCounts.getOrDefault(docId, 0) + count);
        }

        // writing output in the correct format
        for (Map.Entry<String, Integer> entry : docCounts.entrySet()) {
            String docId = entry.getKey();
            int totalCount = entry.getValue();
            context.write(key, new Text(docId + ":" + totalCount)); 
        }
    }
}
