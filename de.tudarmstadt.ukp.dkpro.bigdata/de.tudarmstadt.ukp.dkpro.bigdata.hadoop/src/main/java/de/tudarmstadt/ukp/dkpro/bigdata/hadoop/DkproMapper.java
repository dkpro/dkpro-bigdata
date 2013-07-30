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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.util.ProcessTrace;
import org.apache.uima.util.ProcessTraceEvent;

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

    public DkproMapper()
    {
        super();
    }

    @Override
    public void map(Text key, CASWritable value, OutputCollector<Text, CASWritable> output,
            Reporter reporter)
        throws IOException
    {
        final CAS aCAS = value.getCAS();
        try {
            // let uima process the cas
            final ProcessTrace result = this.engine.process(aCAS);
            for (final ProcessTraceEvent event : result.getEvents()) {
                reporter.incrCounter("uima", "map event " + event.getType(), 1);
            }
            final Text outkey = getOutputKey(key, aCAS);
            value.setCAS(aCAS);
            reporter.incrCounter("uima", "overall doc size", value.getCAS().getDocumentText()
                    .length());
            output.collect(outkey, value);
        }
        catch (final AnalysisEngineProcessException e) {
            reporter.incrCounter("uima", e.toString(), 1);
            e.printStackTrace();
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

}