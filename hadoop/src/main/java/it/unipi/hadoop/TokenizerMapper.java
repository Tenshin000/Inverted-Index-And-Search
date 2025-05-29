package it.unipi.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

import javax.naming.Context;

// INPUT (key: offset, value: doc)
// OUTPUT (key: word, value: doc-id + ":1")

public class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    private Text valueText = new Text();

    // The map method is very similiar to the standard word-count, with difference
    // in the output format, since in this case document name is included.
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String fullFilePath = MyCombineFileRecordReaderWrapper.getCurrentFilePath();

        String currentFilename = "unknown";
        if (fullFilePath != null) {
            Path path = new Path(fullFilePath);
            currentFilename = path.getName();
        }

        String line = value.toString().toLowerCase().replaceAll("[^\\p{L}0-9\\s]", " ");
        String[] tokens = line.split("\\s+");

        for (String token : tokens) {
            if (!token.isEmpty()) {
                word.set(token);
                valueText.set(currentFilename + ":1");
                context.write(word, valueText);
            }
        }
    }
}