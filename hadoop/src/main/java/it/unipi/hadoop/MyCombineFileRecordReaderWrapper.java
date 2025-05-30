package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;      
import org.apache.hadoop.io.Text;              
import org.apache.hadoop.mapreduce.InputSplit; 
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class MyCombineFileRecordReaderWrapper extends RecordReader<LongWritable, Text> {

    private FileSplit mFileSplit;
    private LineRecordReader mDelegate;

    // ThreadLocal to keep track of current file path for mapper thread
    private static ThreadLocal<String> currentFilePath = new ThreadLocal<>();

    public MyCombineFileRecordReaderWrapper(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
        Path path = split.getPath(index);
        long start = split.getOffset(index);
        long length = split.getLength(index);
        String[] locations = split.getLocations();

        mFileSplit = new FileSplit(path, start, length, locations);
        mDelegate = new LineRecordReader();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        mDelegate.initialize(mFileSplit, context);
        currentFilePath.set(mFileSplit.getPath().toString());
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return mDelegate.nextKeyValue();
    }

    @Override
    public LongWritable getCurrentKey() {
        return mDelegate.getCurrentKey();
    }

    @Override
    public Text getCurrentValue() {
        return mDelegate.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return mDelegate.getProgress();
    }

    @Override
    public void close() throws IOException {
        mDelegate.close();
        currentFilePath.remove();
    }

    // Static accessor for mapper to get current file path
    public static String getCurrentFilePath() {
        return currentFilePath.get();
    }
}