package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class InvertedIndexAndSearch {

    // Helper methods
    private static List<Path> resolveInputPaths(FileSystem fs,
                                               String mode,
                                               String localFolder,
                                               String hdfsFolder,
                                               List<String> rawInputs,
                                               String defaultInputDir) throws IOException {
        List<Path> paths = new ArrayList<>();
        switch (mode) {
            case "local-folder":
                for (java.io.File f : new java.io.File(localFolder).listFiles()) {
                    if (f.isFile() && f.getName().endsWith(".txt")) {
                        paths.add(new Path(f.getAbsolutePath()));
                    }
                }
                break;
            case "local-files":
                for (String fp : rawInputs) {
                    paths.add(new Path(fp));
                }
                break;
            case "hdfs-folder":
                Path hf = new Path("hdfs:///user/hadoop/" + hdfsFolder);
                for (FileStatus st : fs.listStatus(hf)) {
                    if (st.isFile() && st.getPath().getName().endsWith(".txt")) {
                        paths.add(st.getPath());
                    }
                }
                break;
            case "hdfs-files":
                for (String fn : rawInputs) {
                    paths.add(new Path("hdfs:///user/hadoop/" + fn));
                }
                break;
            default:
                for (FileStatus st : fs.listStatus(new Path(defaultInputDir))) {
                    if (st.isFile()) {
                        paths.add(st.getPath());
                    }
                }
                break;
        }
        return paths;
    }

    private static List<Path> applyLimit(FileSystem fs,
                                         List<Path> paths,
                                         long limitBytes) throws IOException {
        Collections.sort(paths, Comparator.comparingLong(p -> {
            try { return fs.getFileStatus(p).getLen(); }
            catch (IOException e) { return Long.MAX_VALUE; }
        }));
        List<Path> limited = new ArrayList<>();
        long sum = 0;
        for (Path p : paths) {
            long len = fs.getFileStatus(p).getLen();
            if (sum + len <= limitBytes) {
                limited.add(p);
                sum += len;
            } else {
                break;
            }
        }
        return limited;
    }

    private static Path resolveOutputPath(FileSystem fs,
                                          String defaultHdfsRoot,
                                          String customHdfsRoot,
                                          String outputLocal) throws IOException {
        String root = customHdfsRoot != null ? customHdfsRoot : defaultHdfsRoot;
        Path base = new Path(root);
        String baseName = "output-hadoop";
        int suffix = 0;
        Path candidate;
        do {
            candidate = new Path(base, baseName + suffix);
            suffix++;
        } while (fs.exists(candidate));
        if (outputLocal != null) {
            return new Path(outputLocal, baseName + (suffix - 1));
        }
        return candidate;
    }

    // Main
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // default HDFS paths
        String defaultInputDir = "hdfs:///user/hadoop/inverted-index/data/";
        String defaultOutputRoot = "hdfs:///user/hadoop/inverted-index/output/";

        // CLI state
        int numReducers = -1;
        String inputMode = "default";
        String localFolder = null;
        String hdfsFolder = null;
        List<String> rawInputs = new ArrayList<>();
        long limitBytes = -1;
        String outputLocal = null;
        String outputHdfsRoot = null;
        boolean useStateful = true;      // default: In-Mapper Combiner
        boolean useCombiner = false;     // only with --combiner

        // parse arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--reducers":
                    int r = Integer.parseInt(args[++i]);
                    if (r > 0) numReducers = r;
                    break;
                case "--input-folder":
                    inputMode = "local-folder";
                    localFolder = args[++i];
                    break;
                case "--input-texts":
                    inputMode = "local-files";
                    while (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                        rawInputs.add(args[++i]);
                    }
                    break;
                case "--input-hdfs-folder":
                    inputMode = "hdfs-folder";
                    hdfsFolder = args[++i];
                    break;
                case "--input-hdfs-texts":
                    inputMode = "hdfs-files";
                    while (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                        rawInputs.add(args[++i]);
                    }
                    break;
                case "--limit-mb":
                    limitBytes = Long.parseLong(args[++i]) * 1024L * 1024L;
                    break;
                case "--output":
                    outputLocal = args[++i];
                    break;
                case "--output-hdfs":
                    outputHdfsRoot = "hdfs:///user/hadoop/" + args[++i] + "/";
                    break;
                case "--combiner":
                    useStateful = false;
                    useCombiner = true;
                    break;
                case "--no-combiner":
                    useStateful = false;
                    useCombiner = false;
                    break;
                default:
                    // ignore unknown flags
            }
        }

        // resolve input files
        List<Path> inputPaths = resolveInputPaths(fs, inputMode, localFolder, hdfsFolder, rawInputs, defaultInputDir);

        // apply limit if default mode
        if (limitBytes > 0 && "default".equals(inputMode)) {
            inputPaths = applyLimit(fs, inputPaths, limitBytes);
        }

        // determine output path
        Path outputPath = resolveOutputPath(fs, defaultOutputRoot, outputHdfsRoot, outputLocal);

        // --- JOB CONFIGURATION in main ---
        long startTime = System.currentTimeMillis();
        Job job = Job.getInstance(conf, "HadoopInvertedIndexSearch");
        job.setJarByClass(InvertedIndexAndSearch.class);

        // set mapper based on flags
        if (useStateful) {
            job.setMapperClass(TokenizerMapperStateful.class);
        } else {
            job.setMapperClass(TokenizerMapper.class);
        }

        // set combiner if requested
        if (useCombiner) {
            job.setCombinerClass(CombinerDocCounts.class);
        }

        job.setReducerClass(DocumentCountReducer.class);

        // set number of reducers
        if (numReducers > 0) {
            job.setNumReduceTasks(numReducers);
        }

        // set output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // add inputs
        for (Path in : inputPaths) {
            FileInputFormat.addInputPath(job, in);
        }

        // set output path
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setInputFormatClass(MyCombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 134217728);

        // submit job
        boolean success = job.waitForCompletion(true);

        // execution time 
        long endTime   = System.currentTimeMillis();
        double executionTime = (endTime - startTime) / 1000.0;
        System.out.printf("Execution Time: %.2f s%n", executionTime);
        
        System.exit(success ? 0 : 1);
    }
}
