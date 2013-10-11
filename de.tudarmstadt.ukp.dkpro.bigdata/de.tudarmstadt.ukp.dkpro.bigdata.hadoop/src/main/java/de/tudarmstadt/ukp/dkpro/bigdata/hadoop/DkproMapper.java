/*******************************************************************************
 * Copyright 2012-13
 * TU Darmstadt, UKP Lab
 * with FG Sprachtechnologie
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

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.ProcessTrace;
import org.apache.uima.util.ProcessTraceEvent;

import de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop.BinCasWritable;
import de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop.CASWritable;

/**
 * An "intermediate" reducer for building pipelines with M/R. The engine is _NOT_ supposed to be a
 * CasConsumer, the resulting cas will be written to HDFS and can be used as input to a Mapper
 * process again.
 * 
 * @author zorn
 * 
 * @param <Text>
 */
public class DkproMapper
    extends UIMAMapReduceBase
    implements Mapper<Text, CASWritable, Text, CASWritable>
{
    public enum INPUT_FORMAT
    {
        CAS, TEXT, WEBARCHIVE
    }

    private final Random random;
   
    public DkproMapper()
    {
        super();
        this.random = new Random();
    }

    @Override
    public void map(Text key, CASWritable value, OutputCollector<Text, CASWritable> output,
            Reporter reporter)
        throws IOException
    {
        final CAS aCAS = value.getCAS();

        if (samplingPropability != 100)
            if (random.nextInt(100) >= samplingPropability) {
                reporter.incrCounter("uima", "sampling: SKIPPED", 1);
                return;
            }
        reporter.incrCounter("uima", "sampling: NOT SKIPPED", 1);

      
        try {
            // let uima process the cas
            final ProcessTrace result = this.engine.process(aCAS);
            for (final ProcessTraceEvent event : result.getEvents()) {
                reporter.incrCounter("uima", "map event " + event.getType(), 1);
            }
            final Text outkey = getOutputKey(key, aCAS);
            outValue.setCAS(aCAS);
            if (aCAS.getDocumentText() != null)
                reporter.incrCounter("uima", "overall doc size", outValue.getCAS().getDocumentText()
                        .length());
            output.collect(outkey, outValue);
        }
        catch (final AnalysisEngineProcessException e) {
            reporter.incrCounter("uima", e.toString(), 1);
            if (failures++ > maxFailures)
                throw new IOException(e);

        }
    }

    /**
     * Overwrite this method to generate keys for the map-outputs With the default implementation
     * all cases will be passed through a single reducer, which disables parallelization but has the
     * advantage to have one single output file for the whole collection.
     * 
     * @param aCAS
     * @return
     */
    protected Text getOutputKey(Text key, CAS aCAS)
    {
        return key;
    }

    @Override
    AnalysisEngineDescription getEngineDescription(EngineFactory factory, JobConf job)
        throws ResourceInitializationException
    {
        return factory.buildMapperEngine(job);
    }
    
    @Override
    public void configure(JobConf job) {
    	super.configure(job);
    	try {
    		// create an output writable of the appropriate type
			outValue = (CASWritable) job.getMapOutputValueClass().newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 
    }
}