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

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createAggregate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tools.ant.filters.StringInputStream;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XCASDeserializer;
import org.apache.uima.cas.impl.XCASSerializer;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.ProcessTrace;
import org.apache.uima.util.ProcessTraceEvent;
import org.xml.sax.SAXException;

/**
 * An "intermediate" reducer for building pipelines with M/R. The engine is _NOT_ supposed to be a
 * CasConsumer, the resulting cas will be written to HDFS and can be used as input to a Mapper
 * process again.
 * 
 * @author zorn
 * 
 * @param <K>
 */
public class DkproSimpleMapper<K extends Writable>
    extends MapReduceBase
    implements Mapper<K, Text, K, Text>
{
    private final Log sLogger = LogFactory.getLog(getClass());
    AnalysisEngine engine;
    private int casType;

    public DkproSimpleMapper()
    {
        super();
    }

    @Override
    public void configure(org.apache.hadoop.mapred.JobConf job)
    {

        casType = job.getInt("uima.input.cas.serialization", 0);
        try {

            EngineFactory engineFactory = (EngineFactory) Class.forName(
                    job.get("dkpro.uima.factory", DkproHadoopDriver.class.getCanonicalName()))
                    .newInstance();
            engine = createAggregate(engineFactory.buildMapperEngine(job));
        }
        catch (Exception e) {
            sLogger.fatal("Error while configuring pipeline", e);
            throw new RuntimeException();
        }

    };

    @Override
    public void close()
        throws IOException
    {
        try {
            engine.batchProcessComplete();
        }
        catch (AnalysisEngineProcessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        engine.destroy();
    };

    @Override
    public void map(K key, Text value, OutputCollector<K, Text> output, Reporter reporter)
        throws IOException
    {
        try {
            CAS aCAS = engine.newCAS();
            if (casType == 0)
                XCASDeserializer.deserialize(new StringInputStream(value.toString()), aCAS);
            else
                XmiCasDeserializer.deserialize(new StringInputStream(value.toString()), aCAS);
            ProcessTrace result = engine.process(aCAS);
            for (ProcessTraceEvent event : result.getEvents()) {
                reporter.incrCounter("uima", "map event " + event.getType(), 1);
            }
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            XCASSerializer.serialize(aCAS, outputStream);
            K outkey = getOutputKey(aCAS);
            String string = outputStream.toString();
            // System.out.println(string);
            output.collect(outkey, new Text(string));
        }
        catch (AnalysisEngineProcessException e) {
            reporter.incrCounter("uima", e.toString(), 1);
            e.printStackTrace();
        }
        catch (ResourceInitializationException e) {
            reporter.incrCounter("uima", e.getLocalizedMessage(), 1);
        }
        catch (SAXException e) {
            reporter.incrCounter("uima", e.getLocalizedMessage(), 1);
        }
    }

    protected K getOutputKey(CAS aCAS)
    {
        return (K) new IntWritable(1);
    }

}