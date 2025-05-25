package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
    

// INPUT (key: offset, value: doc)
// OUTPUT (key: word, value: doc-id + ":1")

public class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    private String filename;

// In the setup method, document name is retrieved
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        filename = fileSplit.getPath().getName();
    }

// The map method is very similiar to the standard word-count, with difference in the output format, since in this case document name is included.
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase().replaceAll("[.,:;]\'\"", "");
        String[] tokens = line.split("\\s+");
        for (String token : tokens) {
            if (!token.isEmpty()) {
                word.set(token);
                context.write(word, new Text(filename + ":1"));
            }
        }
    }
}