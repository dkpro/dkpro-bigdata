/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dkpro.bigdata.collocations;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.dkpro.bigdata.collocations.CollocMapper.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Driver for LLR Collocation discovery mapreduce job */
public final class CollocDriver
    extends AbstractJob
{
    // public static final String DEFAULT_OUTPUT_DIRECTORY = "output";

    public static final String SUBGRAM_OUTPUT_DIRECTORY = "subgrams";

    public static final String NGRAM_OUTPUT_DIRECTORY = "ngrams";

    public static final String EMIT_UNIGRAMS = "emit-unigrams";

    public static final boolean DEFAULT_EMIT_UNIGRAMS = false;

    private static final int DEFAULT_MAX_NGRAM_SIZE = 2;

    private static final int DEFAULT_PASS1_NUM_REDUCE_TASKS = 1;

    private static final Logger log = LoggerFactory.getLogger(CollocDriver.class);

    public static final String WINDOW_SIZE = "colloc.window";
    public static final String WINDOW_TYPE = "colloc.window_type";

    public static void main(String[] args)
        throws Exception
    {
        ToolRunner.run(new CollocDriver(), args);
    }

    @Override
    public int run(String[] args)
        throws Exception
    {
        addInputOption();
        addOutputOption();
        addOption(DefaultOptionCreator.numReducersOption().create());

        addOption(
                "maxNGramSize",
                "ng",
                "(Optional) The max size of ngrams to create (2 = bigrams, 3 = trigrams, etc) default: 2",
                String.valueOf(DEFAULT_MAX_NGRAM_SIZE));
        addOption("minSupport", "s", "(Optional) Minimum Support. Default Value: "
                + CollocReducer.DEFAULT_MIN_SUPPORT,
                String.valueOf(CollocReducer.DEFAULT_MIN_SUPPORT));
        addOption("minValue", "minV",
                "(Optional)The minimum value for association metric(Float)  Default is "
                        + AssocReducer.DEFAULT_MIN_VALUE,
                String.valueOf(AssocReducer.DEFAULT_MIN_VALUE));
        addOption(DefaultOptionCreator.overwriteOption().create());
        addOption("metric", "m", "The association metric to use, one of {llr,dice,pmi,chi}",
                AssocReducer.DEFAULT_ASSOC);
        addFlag("unigram", "u",
                "If set, unigrams will be emitted in the final output alongside collocations");
        addOption("windowSize", "ws", "(Optional) Window size");
        addOption("windowMode", "wm", "(Optional) DOCUMENT, SENTENCE, S_WINDOW, C_WINDOW, FIXED");
        addOption("ngramLimit", "nl",
                "(Optional) maximum of ngrams per unit - to prevent memory overflow");
        addOption("usePos", "p", "(Optional)");
        Map<String, List<String>> argMap = parseArguments(args);

        if (argMap == null) {
            return -1;
        }

        Path input = getInputPath();
        Path output = getOutputPath();

        int maxNGramSize = DEFAULT_MAX_NGRAM_SIZE;
        if (hasOption("maxNGramSize")) {
            try {
                maxNGramSize = Integer.parseInt(getOption("maxNGramSize"));
            }
            catch (NumberFormatException ex) {
                log.warn("Could not parse ngram size option");
            }
        }
        log.info("Maximum n-gram size is: {}", maxNGramSize);

        if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
            HadoopUtil.delete(getConf(), output);
        }

        int minSupport = CollocReducer.DEFAULT_MIN_SUPPORT;
        if (getOption("minSupport") != null) {
            minSupport = Integer.parseInt(getOption("minSupport"));
        }
        log.info("Minimum Support value: {}", minSupport);

        float minValue = AssocReducer.DEFAULT_MIN_VALUE;
        if (getOption("minValue") != null) {
            minValue = Float.parseFloat(getOption("minValue"));
        }
        log.info("Minimum Assoc value: {}", minValue);

        int reduceTasks = DEFAULT_PASS1_NUM_REDUCE_TASKS;
        if (getOption("maxRed") != null) {
            reduceTasks = Integer.parseInt(getOption("maxRed"));
        }
        log.info("Number of pass1 reduce tasks: {}", reduceTasks);

        String metric = AssocReducer.DEFAULT_ASSOC;
        if (getOption("metric") != null) {
            metric = getOption("metric");
        }
        log.info("Association Metric: {}", metric);
        Window windowType = Window.SENTENCE;
        if (getOption("windowMode") != null) {
            windowType = Window.valueOf(getOption("windowMode").toUpperCase());
        }
        int windowSize = 3;
        if (getOption("windowSize") != null) {
            windowSize = Integer.parseInt(getOption("windowSize"));
        }

        boolean emitUnigrams = argMap.containsKey("emitUnigrams");
        reduceTasks = 14;
        // parse input and extract collocations
        long ngramCount = generateCollocations(input, output, getConf(), emitUnigrams,
                maxNGramSize, reduceTasks, minSupport, windowType, windowSize);

        // tally collocations and perform LLR calculation
        // for (String m : metric.split(",")) {
        // log.info("Computing Collocations with Association Metric: {}", m);
        // // extract pruning thresholds
        // if (m.contains(":")) {
        // String[] tokens = m.split(":");
        // m = tokens[0];
        // minValue = Float.parseFloat(tokens[1]);
        // }

        computeNGramsPruneByLLR(output, getConf(), ngramCount, emitUnigrams, minValue, reduceTasks);
        // only emit unigrams for the first metric
        emitUnigrams = false;
        // }
        return 0;
    }

    /**
     * Generate all ngrams for the {@link org.apache.mahout.vectorizer.DictionaryVectorizer} job
     * 
     * @param input
     *            input path containing tokenized documents
     * @param output
     *            output path where ngrams are generated including unigrams
     * @param baseConf
     *            job configuration
     * @param maxNGramSize
     *            minValue = 2.
     * @param minSupport
     *            minimum support to prune ngrams including unigrams
     * @param minLLRValue
     *            minimum threshold to prune ngrams
     * @param reduceTasks
     *            number of reducers used
     */
    public static void generateAllGrams(Path input, Path output, Configuration baseConf,
            int maxNGramSize, int minSupport, float minLLRValue, int reduceTasks, String metric,
            Window windowMode, int windowSize)
        throws IOException, InterruptedException, ClassNotFoundException
    {
        // parse input and extract collocations
        long ngramCount = generateCollocations(input, output, baseConf, true, maxNGramSize,
                reduceTasks, minSupport, windowMode, windowSize);

        // tally collocations and perform LLR calculation
        computeNGramsPruneByLLR(output, baseConf, ngramCount, true, minLLRValue, reduceTasks);
    }

    /**
     * pass1: generate collocations, ngrams
     */
    private static long generateCollocations(Path input, Path output, Configuration baseConf,
            boolean emitUnigrams, int maxNGramSize, int reduceTasks, int minSupport, Window mode,
            int winsize)
        throws IOException, ClassNotFoundException, InterruptedException
    {

        Configuration con = new Configuration(baseConf);
        con.setBoolean(EMIT_UNIGRAMS, emitUnigrams);
        con.setInt(CollocMapper.MAX_SHINGLE_SIZE, maxNGramSize);
        con.setInt(CollocReducer.MIN_SUPPORT, minSupport);
        con.set(WINDOW_TYPE, mode.toString());
        con.setInt(WINDOW_SIZE, winsize);

        if (mode.toString().equalsIgnoreCase("DOCUMENT")) {
            con.setInt("mapred.job.map.memory.mb", 3000);

            con.set("mapred.child.java.opts", "-Xmx2900M");
            con.set("mapred.reduce.child.java.opts", "-Xmx8000M");

            con.setInt("mapred.job.reduce.memory.mb", 8120);
        }
        else {
            con.setInt("mapred.job.map.memory.mb", 2000);

            con.set("mapred.child.java.opts", "-Xmx1900M");
            con.set("mapred.reduce.child.java.opts", "-Xmx2900M");

            con.setInt("mapred.job.reduce.memory.mb", 3000);
        }
        con.setBoolean("mapred.compress.map.output", true);
        con.setStrings("mapred.map.output.compression.codec",
                "org.apache.hadoop.io.compress.DefaultCodec");
        con.setBoolean("mapred.compress.output", true);
        con.setStrings("mapred.output.compression.codec",
                "org.apache.hadoop.io.compress.DefaultCodec");
        con.setInt("mapred.task.timeout", 6000000);
        con.setInt("io.sort.factor", 50);
        con.setInt("mapreduce.map.tasks", 256);
        con.setInt("dfs.replication", 1);
        Job job = new Job(con);
        job.setJobName(CollocDriver.class.getSimpleName() + ".generateCollocations:" + input);
        job.setJarByClass(CollocDriver.class);

        job.setMapOutputKeyClass(GramKey.class);
        job.setMapOutputValueClass(Gram.class);
        job.setPartitionerClass(GramKeyPartitioner.class);
        job.setGroupingComparatorClass(GramKeyGroupComparator.class);

        job.setOutputKeyClass(Gram.class);
        job.setOutputValueClass(Gram.class);

        job.setCombinerClass(CollocCombiner.class);

        FileInputFormat.setInputPaths(job, input);

        Path outputPath = new Path(output, SUBGRAM_OUTPUT_DIRECTORY);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(CollocMapper.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setReducerClass(CollocReducer.class);
        job.setNumReduceTasks(512);

        boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new IllegalStateException("Job failed!");
        }

        return job.getCounters().findCounter(CollocMapper.Count.NGRAM_TOTAL).getValue();
    }

    /**
     * pass2: perform the LLR calculation
     */
    private static void computeNGramsPruneByLLR(Path output, Configuration baseConf,
            long nGramTotal, boolean emitUnigrams, float minValue, int reduceTasks)
        throws IOException, InterruptedException, ClassNotFoundException
    {
        Configuration conf = new Configuration(baseConf);
        conf.setLong(AssocReducer.NGRAM_TOTAL, nGramTotal);
        conf.setBoolean(EMIT_UNIGRAMS, emitUnigrams);
        conf.setFloat(AssocReducer.MIN_VALUE, minValue);
        conf.setInt("mapred.job.map.memory.mb", 1280);
        conf.setInt("mapred.job.reduce.memory.mb", 2560);
        conf.set("mapred.reduce.child.java.opts", "-Xmx2G");
        conf.setInt("mapred.task.timeout", 6000000);
        conf.set(AssocReducer.ASSOC_METRIC, "llr");

        Job job = new Job(conf);
        job.setJobName(CollocDriver.class.getSimpleName() + ".computeNGrams: " + output
                + " pruning: " + minValue);
        job.setJarByClass(CollocDriver.class);

        job.setMapOutputKeyClass(Gram.class);
        job.setMapOutputValueClass(Gram.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.setInputPaths(job, new Path(output, SUBGRAM_OUTPUT_DIRECTORY));
        Path outPath = new Path(output, NGRAM_OUTPUT_DIRECTORY + "_llr");
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(Mapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        job.setReducerClass(AssocReducer.class);
        job.setNumReduceTasks(reduceTasks);
        // Defines additional single text based output 'text' for the job
        MultipleOutputs.addNamedOutput(job, "contingency", TextOutputFormat.class, Text.class,
                Text.class);

        // Defines additional multi sequencefile based output 'sequence' for the
        // job
        MultipleOutputs.addNamedOutput(job, "llr", TextOutputFormat.class, Text.class,
                DoubleWritable.class);
        MultipleOutputs.addNamedOutput(job, "pmi", TextOutputFormat.class, Text.class,
                DoubleWritable.class);
        MultipleOutputs.addNamedOutput(job, "chi", TextOutputFormat.class, Text.class,
                DoubleWritable.class);
        MultipleOutputs.addNamedOutput(job, "dice", TextOutputFormat.class, Text.class,
                DoubleWritable.class);

        boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new IllegalStateException("Job failed!");
        }
    }

}
