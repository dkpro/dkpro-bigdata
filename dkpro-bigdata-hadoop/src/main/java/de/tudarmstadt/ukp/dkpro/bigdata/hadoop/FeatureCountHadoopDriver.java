/*******************************************************************************
 * Copyright 2013
 * TU Darmstadt, FG Sprachtechnologie
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package de.tudarmstadt.ukp.dkpro.bigdata.hadoop;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;

import de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop.CASWritable;

/**
 * Base class for counting features (n-grams, cooccurrences, etc.) from serialized CAS instances.
 * 
 * @author Johannes Simon
 * 
 */
public abstract class FeatureCountHadoopDriver
    extends Configured
    implements Tool
{

    /**
     * Used by FeatureCountHadoopDriver to map each CAS to a set of features, e.g. its n-grams or
     * cooccurrences.
     */
    public interface CountableFeatureExtractor
    {
        public void configure(JobConf job);

        public Collection<Text> extract(JCas aJCas);
    }

    /**
     * Maps CAS instances to a set of features extracted using a custom CountableFeatureExtractor
     * implementation.
     */
    private static class CountableFeatureMapper
        extends MapReduceBase
        implements Mapper<Text, CASWritable, Text, LongWritable>
    {
        private CountableFeatureExtractor featureExtractor;

        @Override
        public void configure(JobConf job)
        {
            String featureExtractorClass = job.get("dkpro.uima.countablefeatureextractor");
            if (featureExtractorClass != null) {
                try {
                    featureExtractor = (CountableFeatureExtractor) Class.forName(
                            featureExtractorClass).newInstance();
                    featureExtractor.configure(job);
                }
                catch (InstantiationException e) {
                    throw new RuntimeException(e);
                }
                catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
                catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void map(Text key, CASWritable value, OutputCollector<Text, LongWritable> output,
                Reporter reporter)
            throws IOException
        {
            final CAS aCAS = value.getCAS();
            try {
                Collection<Text> features = featureExtractor.extract(aCAS.getJCas());
                for (Text feature : features) {
                    output.collect(feature, new LongWritable(1));
                }
            }
            catch (CASException e) {
                reporter.incrCounter("uima", e.toString(), 1);
            }
        }
    }

    /**
     * Reduces all occurrences of one feature to its frequency.
     */
    private static class CountableFeatureReducer
        extends MapReduceBase
        implements Reducer<Text, LongWritable, Text, LongWritable>
    {
        @Override
        public void reduce(Text key, Iterator<LongWritable> values,
                OutputCollector<Text, LongWritable> collector, Reporter reporter)
            throws IOException
        {
            long count = 0;
            while (values.hasNext()) {
                count += values.next().get();
            }
            collector.collect(key, new LongWritable(count));
        }
    }

    private JobConf job;

    public abstract Class<? extends CountableFeatureExtractor> getCountableFeatureExtractorClass();

    /**
     * Implement this method to configure your job.
     * 
     * @param job
     */
    public abstract void configure(JobConf job);

    @Override
    public int run(String[] args)
        throws Exception
    {

        if (args.length < 2) {
            System.out.println("Usage: " + this.getClass().getSimpleName()
                    + " [hadoop-params] input output [job-params]");
            System.exit(1);
        }
        this.job = new JobConf(getConf(), DkproHadoopDriver.class);

        // set the factory class name
        this.job.set("dkpro.uima.countablefeatureextractor", getCountableFeatureExtractorClass()
                .getName());

        final Path inputPath = new Path(args[0]);
        final Path outputPath = new Path(args[1]);

        // this is a sensible default for the UKP cluster
        int numMappers = 76;
        int numReducers = 76;

        FileInputFormat.setInputPaths(this.job, inputPath);
        FileOutputFormat.setOutputPath(this.job, outputPath);

        // setup some sensible defaults
        this.job.setMapperClass(CountableFeatureMapper.class);
        this.job.setCombinerClass(CountableFeatureReducer.class);
        this.job.setReducerClass(CountableFeatureReducer.class);
        this.job.setInputFormat(SequenceFileInputFormat.class);
        this.job.setOutputFormat(TextOutputFormat.class);
        this.job.setMapOutputKeyClass(Text.class);
        this.job.setMapOutputValueClass(LongWritable.class);
        this.job.setOutputKeyClass(Text.class);
        this.job.setOutputValueClass(LongWritable.class);
        this.job.setJobName(this.getClass().getSimpleName());
        this.job.setInt("mapred.job.map.memory.mb", 1280);
        this.job.setInt("mapred.job.reduce.memory.mb", 1280);
        this.job.setNumMapTasks(numMappers);
        this.job.setNumReduceTasks(numReducers);
        configure(this.job);

        // create symlinks for distributed resources
        DistributedCache.createSymlink(this.job);
            // sLogger.info("Running job "+job.getJobName());

        JobClient.runJob(this.job);

        return 0;
    }

}
