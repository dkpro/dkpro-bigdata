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
package de.tudarmstadt.ukp.dkpro.core.hadoop;

import static org.apache.uima.fit.factory.CollectionReaderFactory.*;
import static org.apache.uima.fit.factory.AnalysisEngineFactory.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.resource.ResourceInitializationException;

import de.tudarmstadt.ukp.dkpro.bigdata.hadoop.DkproHadoopDriver;
import de.tudarmstadt.ukp.dkpro.bigdata.hadoop.EngineFactory;
import de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop.CASWritable;
import de.tudarmstadt.ukp.dkpro.core.io.text.TextReader;
import de.tudarmstadt.ukp.dkpro.core.io.text.TextWriter;
import de.tudarmstadt.ukp.dkpro.core.tokit.BreakIteratorSegmenter;

public final class MyDriver
    extends DkproHadoopDriver
{

    /**
     * @param dkproHadoopDriverTest
     */
    public MyDriver()
    {
        super();
    }

    @Override
    public CollectionReader buildCollectionReader()
        throws ResourceInitializationException
    {
        return null;
        // return createReader(TextReader.class, TextReader.PARAM_PATH, "src/test/resources/text/");
        // TextReader.PARAM_PATTERNS, new String[] { "[+]*.txt" });
    }

    @Override
    public AnalysisEngineDescription buildMapperEngine(Configuration job)
        throws ResourceInitializationException
    {
        AnalysisEngineDescription tokenizer = createPrimitiveDescription(BreakIteratorSegmenter.class);
        AnalysisEngineDescription aggregate = createAggregateDescription(tokenizer);
        return aggregate;
    }

    @Override
    public AnalysisEngineDescription buildReducerEngine(Configuration job)
        throws ResourceInitializationException
    {
        return createAggregateDescription(createPrimitiveDescription(TextWriter.class,
                TextWriter.PARAM_TARGET_LOCATION, "$dir/output"));
    }

    @Override
    public void configure(JobConf job)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public Class<? extends EngineFactory> getEngineFactoryClass()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Class<? extends InputFormat<Text, CASWritable>> getInputFormatClass()
    {
        // TODO Auto-generated method stub
        return null;
    }

}