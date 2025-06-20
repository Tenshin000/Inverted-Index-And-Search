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

/**
 * A custom wrapper that allows reading each file in a CombineFileSplit
 * using a LineRecordReader. It reads lines and provides (offset, line) pairs
 * from the current file segment.
 */
public class MyCombineFileRecordReaderWrapper extends RecordReader<LongWritable, Text> {
    // A split representing a specific portion of a single file
    private FileSplit mFileSplit;

    // Delegate reader that reads lines from the file
    private LineRecordReader mDelegate;

    // Used to store the current file path for each thread (useful in the Mapper)
    private static ThreadLocal<String> currentFilePath = new ThreadLocal<>();

    /**
     * Constructor called by CombineFileRecordReader to process each file chunk.
     *
     * @param split   The combined split containing multiple files
     * @param context The job/task context
     * @param index   Index of the specific file in the split to read
     */
    public MyCombineFileRecordReaderWrapper(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
        // Extract path, offset, and length for the indexed file segment
        Path path = split.getPath(index);
        long start = split.getOffset(index);
        long length = split.getLength(index);
        String[] locations = split.getLocations();

        // Create a FileSplit for the given segment
        mFileSplit = new FileSplit(path, start, length, locations);

        // Use a LineRecordReader to read the file line by line
        mDelegate = new LineRecordReader();
    }

    /**
     * Initializes the delegate LineRecordReader with the file split.
     * Also sets the current file path in ThreadLocal for use in the Mapper.
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        mDelegate.initialize(mFileSplit, context);
        currentFilePath.set(mFileSplit.getPath().toString());
    }

    /**
     * Reads the next key-value pair (line offset, line content).
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return mDelegate.nextKeyValue();
    }

    /**
     * Returns the current line offset in the file.
     */
    @Override
    public LongWritable getCurrentKey() {
        return mDelegate.getCurrentKey();
    }

    /**
     * Returns the current line content.
     */
    @Override
    public Text getCurrentValue() {
        return mDelegate.getCurrentValue();
    }

    /**
     * Returns the progress of reading this file segment.
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return mDelegate.getProgress();
    }

    /**
     * Closes the reader and clears the ThreadLocal file path.
     */
    @Override
    public void close() throws IOException {
        mDelegate.close();
        currentFilePath.remove();
    }

    /**
     * Provides access to the current file path being read.
     * Useful for the Mapper to know which file the record came from.
     */
    public static String getCurrentFilePath() {
        return currentFilePath.get();
    }
}
