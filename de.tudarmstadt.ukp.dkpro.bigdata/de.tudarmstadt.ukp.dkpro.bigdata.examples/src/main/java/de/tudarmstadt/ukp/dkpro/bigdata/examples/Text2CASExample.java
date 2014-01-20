/*******************************************************************************
 * Copyright 2013
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
package de.tudarmstadt.ukp.dkpro.bigdata.examples;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.*;
import static org.apache.uima.fit.factory.CollectionReaderFactory.*;
import static de.tudarmstadt.ukp.dkpro.core.api.io.ResourceCollectionReaderBase.INCLUDE_PREFIX;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.resource.ResourceInitializationException;

import de.tudarmstadt.ukp.dkpro.bigdata.hadoop.DkproHadoopDriver;
import de.tudarmstadt.ukp.dkpro.bigdata.hadoop.DkproMapper;
import de.tudarmstadt.ukp.dkpro.bigdata.hadoop.DkproReducer;
import de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop.Text2CASInputFormat;
import de.tudarmstadt.ukp.dkpro.core.io.text.TextReader;
import de.tudarmstadt.ukp.dkpro.core.snowball.SnowballStemmer;
import de.tudarmstadt.ukp.dkpro.core.tokit.BreakIteratorSegmenter;
import de.tudarmstadt.ukp.dkpro.core.dictionaryannotator.DictionaryAnnotator;
import de.tudarmstadt.ukp.dkpro.core.examples.type.Name;


/**
 * This example reads plain text files from the HDFS and processes
 * them using a pipeline
 * 
 * When using a special input format, such as Text2CAS, do not use
 * buildCollectionReader, just use
 * 
 * @author hpzorn@gmail.com
 *
 */
public class Text2CASExample
    extends DkproHadoopDriver
{
   

	public AnalysisEngineDescription buildMapperEngine(Configuration job)
        throws ResourceInitializationException
    {
        AnalysisEngineDescription tokenizer = createEngineDescription(BreakIteratorSegmenter.class);
        AnalysisEngineDescription stemmer = createEngineDescription(SnowballStemmer.class,
                SnowballStemmer.PARAM_LANGUAGE, "en");
        return createEngineDescription(tokenizer, stemmer);

    }  
    public static void main(String[] args) throws Exception
    {
            Text2CASExample pipeline = new Text2CASExample();
            pipeline.setMapperClass(DkproMapper.class);
            pipeline.setReducerClass(DkproReducer.class);
            ToolRunner.run(new Configuration(), pipeline, args);
    }
	@Override
	public void configure(JobConf job) {
		job.set("mapreduce.job.queuename", "smalljob");	
		/*
		 * Use Text2Cas InputFormat, read texts directly from h
		 */
		
		job.setInputFormat(Text2CASInputFormat.class);
	}
	
	@Override
	public Class getInputFormatClass() {
		// TODO Auto-generated method stub
		return null;
	}

}
