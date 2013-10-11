/*******************************************************************************
 * Copyright 2012
 * TU Darmstadt, UKP Lab 
 * with  FG Sprachtechnologie
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
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.ProcessTrace;
import org.apache.uima.util.ProcessTraceEvent;

import de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop.CASWritable;

/**
 * This class expects a UIMA Consumer as engine, it will not collect the output but will copy
 * everything from the local directory to HDFS after completion. Useful for e.g. LuceneIndexWriter
 * 
 * @author zorn
 * 
 */
public class DkproReducer
    extends UIMAMapReduceBase
    implements Reducer<Writable, CASWritable, Writable, CASWritable>
{

    Log logger = LogFactory.getLog("DkproReducer");
    private int casType;

    @Override
    public void reduce(Writable key, Iterator<CASWritable> values,
            OutputCollector<Writable, CASWritable> output, Reporter reporter)
        throws IOException
    {

        while (values.hasNext()) {
            final CAS aCAS = values.next().getCAS();
            try {
                // let uima process the cas
                final ProcessTrace result = this.engine.process(aCAS);
                for (final ProcessTraceEvent event : result.getEvents()) {
                    reporter.incrCounter("uima", "map event " + event.getType(), 1);
                }
              
              
                outValue.setCAS(aCAS);
                reporter.incrCounter("uima", "overall doc size", outValue.getCAS().getDocumentText()
                        .length());
                output.collect(key, outValue);
            }
            catch (final AnalysisEngineProcessException e) {
                reporter.incrCounter("uima", e.toString(), 1);
                if (failures++ > maxFailures)
                    throw new IOException(e);

            }
        }
    }

    @Override
    AnalysisEngineDescription getEngineDescription(EngineFactory factory, JobConf job)
        throws ResourceInitializationException
    {
        return factory.buildReducerEngine(job);
    }
    @Override
    public void configure(JobConf job) {
    	super.configure(job);
    	try {
    		// create an output writable of the appropriate type
			outValue = (CASWritable) job.getOutputValueClass().newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 
    }
}