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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.impl.XCASDeserializer;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.ProcessTrace;
import org.apache.uima.util.ProcessTraceEvent;
import org.xml.sax.SAXException;

/**
 * This class expects a UIMA Consumer as engine, it will not collect the output but will copy
 * everything from the local directory to HDFS after completion. Useful for e.g. LuceneIndexWriter
 * 
 * @author zorn
 * 
 */
public class DkproReducer
    extends UIMAMapReduceBase
    implements Reducer<Writable, Text, Writable, Text>
{

    Log logger = LogFactory.getLog("DkproReducer");
    private int casType;

    @Override
    public void reduce(Writable key, Iterator<Text> values, OutputCollector<Writable, Text> output,
            Reporter reporter)
        throws IOException
    {

        while (values.hasNext())
            try {
                Text value = values.next();
                CAS aCAS = engine.newCAS();

                XCASDeserializer.deserialize(
                        new ByteArrayInputStream(value.toString().getBytes("UTF-8")), aCAS);
                // XmiCasSerializer.serialize(aCAS, new FileOutputStream(key.toString()+".xmi"));

                ProcessTrace result = engine.process(aCAS.getJCas());
                for (ProcessTraceEvent event : result.getEvents()) {
                    reporter.incrCounter("uima", "reduce event:" + event.getType(), 1);
                }
                // ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                // XmiCasSerializer.serialize(aCAS,outputStream);
                // output.collect(key, new Text(outputStream.toString()));
            }
            catch (AnalysisEngineProcessException e) {
                reporter.incrCounter("uima", "exception", 1);

            }
            catch (ResourceInitializationException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (SAXException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (CASException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
    }

}