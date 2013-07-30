/*******************************************************************************
 * Copyright 2012
 * Ubiquitous Knowledge Processing (UKP) Lab
 * Technische Universität Darmstadt
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package de.tudarmstadt.ukp.dkpro.bigdata.hadoop;

import static org.uimafit.factory.TypeSystemDescriptionFactory.createTypeSystemDescription;
import static org.uimafit.pipeline.SimplePipeline.runPipeline;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.resource.ResourceInitializationException;
import org.uimafit.factory.AnalysisEngineFactory;

import de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop.CASWritable;
import de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop.CASWritableSequenceFileWriter;

/**
 * Base class for running UIMA Pipelines on the cluster, see also
 * https://maggie/wiki/bin/view/DKPro/ExecutingDKProPipelinesOnHadoop
 * 
 * @author zorn
 * 
 */
public abstract class DkproHadoopDriver
    extends Configured
    implements Tool, EngineFactory
{
    private Class<? extends DkproMapper> mapperClass = DkproMapper.class;
    private Class<? extends DkproReducer> reducerClass = DkproReducer.class;

    private JobConf job;

    /**
     * usually the dkproHadoopDriver implementation should also implement EngineFactory, uses less
     * boilerplate code
     * 
     * @return reference to self's class
     */
    public Class getEngineFactoryClass()
    {
        return this.getClass();
    };

    public abstract Class getInputFormatClass();

    /**
     * 
     * 
     * get the mapper implementation
     * 
     * @return
     */
    public Class<? extends DkproMapper> getMapperClass()
    {
        return this.mapperClass;
    }

    /**
     * 
     * Set a custom mapper implementation
     * 
     * @param mapperClass
     */
    public void setMapperClass(Class<? extends DkproMapper> mapperClass)
    {
        this.mapperClass = mapperClass;
    }

    public Class<? extends DkproReducer> getReducerClass()
    {
        return this.reducerClass;
    }

    /**
     * Set a custom reducer implementation
     * 
     * @param reducerClass
     */
    public void setReducerClass(Class<? extends DkproReducer> reducerClass)
    {
        this.reducerClass = reducerClass;
    }

    /**
     * Implement this method to configure your job.
     * 
     * @param job
     */
    @Override
    public abstract void configure(JobConf job);

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
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
        final FileSystem fs = FileSystem.get(this.job);
        Class.forName("de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop.CASWritable");
        // set the factory class name
        this.job.set("dkpro.uima.factory", getEngineFactoryClass().getName());

        final Path inputPath = new Path(args[0]); // input
        final Path outputPath = new Path(args[1]);// output
        final CollectionReader reader = buildCollectionReader();
        // if a collection reader was defined, import data into hdfs
        // try {
        // final Class<?> c = Class.forName("org.apache.hadoop.io.compress.SnappyCodec");
        // FileOutputFormat.setOutputCompressorClass(this.job,
        // (Class<? extends CompressionCodec>) c);
        // }
        // catch (final Exception e) {
        //
        // }
        if (reader != null) {
            final AnalysisEngine xcasWriter = AnalysisEngineFactory.createPrimitive(
                    CASWritableSequenceFileWriter.class, createTypeSystemDescription(),
                    CASWritableSequenceFileWriter.PARAM_PATH, inputPath.toString(),
                    CASWritableSequenceFileWriter.PARAM_COMPRESS, true,
                    CASWritableSequenceFileWriter.PARAM_FS, job.get(("fs.default.name"), "file:/"));
            runPipeline(reader, xcasWriter);
        }
        // cleanup previous output
        fs.delete(outputPath, true);
        // this is a sensible default for the UKP cluster
        int numMappers = 256;
        // if (args.length > 2) {
        // numMappers = Integer.parseInt(args[2]);
        // }

        FileInputFormat.setInputPaths(this.job, inputPath);

        FileOutputFormat.setOutputPath(this.job, outputPath);
        // SequenceFileOutputFormat.setCompressOutput(this.job, true);

        if (this.job.get("mapred.output.compress") == null) {
            this.job.setBoolean("mapred.output.compress", true);
        }
        // Just in case compression is on
        this.job.set("mapred.output.compression.type", "BLOCK");

        if (this.job.getBoolean("dkpro.output.plaintext", false)) {
            this.job.setOutputFormat(TextOutputFormat.class);
        }
        else {
            this.job.setOutputFormat(SequenceFileOutputFormat.class);
        }
        // this.job.set("mapred.output.compression.codec",
        // "org.apache.hadoop.io.compress.GzipCodec");
        // use compression
        // setup some sensible defaults
        this.job.setMapperClass(this.mapperClass);
        this.job.setReducerClass(this.reducerClass);
        if (getInputFormatClass() != null) {
            this.job.setInputFormat(getInputFormatClass());
        }
        else {
            this.job.setInputFormat(SequenceFileInputFormat.class);
        }
        // this.job.setOutputFormat(TextOutputFormat.class);
        this.job.setMapOutputKeyClass(Text.class);
        this.job.setMapOutputValueClass(CASWritable.class);
        this.job.setOutputKeyClass(Text.class);
        this.job.setOutputValueClass(CASWritable.class);
        this.job.setJobName(this.getClass().getSimpleName());
        // this.job.set("mapred.child.java.opts", "-Xmx1g");
        this.job.setInt("mapred.job.map.memory.mb", 1280);
        this.job.setInt("mapred.job.reduce.memory.mb", 1280);
        this.job.setNumMapTasks(numMappers);
        this.job.setNumReduceTasks(0);
        configure(this.job);

        // create symlinks for distributed resources
        DistributedCache.createSymlink(this.job);
        // sLogger.info("Running job "+job.getJobName());

        JobClient.runJob(this.job);

        return 0;

    }

    /**
     * Register a data archive to be distributed to the distributed cache. The resource can than be
     * accessed from any UIMA component by specifying $name within the configuration.
     * 
     * Archives that are bigger than 4 GB need to be .tar.gz, because Java6 zip implementation does
     * not support zip> 4GB
     * 
     * For External Resources, the ER has to be setup using job.getResource("name") in the
     * build*Engine method.
     * 
     * 
     * @param name
     *            identifier for the arcive
     * @param uri
     *            URI of the archive, can be file:/... or hdfs://...
     */
    public void registerDataArchive(String name, URI uri)
    {
        try {
            DistributedCache.addCacheArchive(new URI(uri.toString() + "#" + name), this.job);
            String resources = this.job.get("dkpro.resources", "");
            if (!resources.isEmpty()) {
                resources += ",";
            }
            resources += name;
            this.job.set("dkpro.resources", resources);

        }
        catch (final URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * Overwrite this method if you need to import data using a UIMA collection reader
     * 
     * @return
     * @throws ResourceInitializationException
     */
    public CollectionReader buildCollectionReader()
        throws ResourceInitializationException
    {
        return null;
    }

}
