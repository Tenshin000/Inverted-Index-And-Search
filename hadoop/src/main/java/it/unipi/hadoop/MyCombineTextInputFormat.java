package it.unipi.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * Custom InputFormat to combine multiple small text files into larger splits.
 * Uses CombineFileInputFormat to reduce the number of map tasks for small files.
 */
public class MyCombineTextInputFormat extends CombineFileInputFormat<LongWritable, Text> {

    /**
     * Create a RecordReader for the given InputSplit.
     * This method wraps each split (which may contain multiple files) into a CombineFileRecordReader,
     * using our custom FileRecordReader wrapper to read lines and track file boundaries.
     *
     * @param split   the combined input split containing multiple file paths
     * @param context the task attempt context
     * @return a RecordReader that will iterate over (offset, line) pairs across the split
     * @throws IOException if initialization fails
     */
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        // Cast the generic InputSplit to CombineFileSplit, which holds multiple file segments
        CombineFileSplit combineSplit = (CombineFileSplit) split;

        // Instantiate a CombineFileRecordReader that delegates reading each file in the split
        // to our MyCombineFileRecordReaderWrapper, which handles per-file line reading and tracking
        return new CombineFileRecordReader<LongWritable, Text>(
                combineSplit,
                context,
                MyCombineFileRecordReaderWrapper.class);
    }
}
