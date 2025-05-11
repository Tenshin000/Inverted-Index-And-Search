package com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class InvertedIndexAndSearch {
    public static void main(String[] args) throws Exception {
	if (args.length < 2) {
        	System.err.println("Usage: InvertedIndexAndSearch <input path> <output path>");
        	System.exit(-1);
    	}
    	String[] otherArgs = new String[args.length];
    	System.arraycopy(args, 0, otherArgs, 0, args.length);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index and Search");

        job.setJarByClass(InvertedIndexAndSearch.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(DocumentCountReducer.class);
        job.setReducerClass(DocumentCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
