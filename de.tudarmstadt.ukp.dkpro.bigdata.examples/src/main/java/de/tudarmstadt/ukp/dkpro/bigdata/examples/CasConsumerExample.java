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
import de.tudarmstadt.ukp.dkpro.core.io.text.TextWriter;
import de.tudarmstadt.ukp.dkpro.core.snowball.SnowballStemmer;
import de.tudarmstadt.ukp.dkpro.core.tokit.BreakIteratorSegmenter;
import de.tudarmstadt.ukp.dkpro.core.dictionaryannotator.DictionaryAnnotator;
//import de.tudarmstadt.ukp.dkpro.core.examples.type.Name;
public class CasConsumerExample
    extends DkproHadoopDriver
{
   
//    public CollectionReader buildCollectionReader()
//        throws ResourceInitializationException
//    {
//        return createReader(TextReader.class, TextReader.PARAM_SOURCE_LOCATION, "src/test/resources/text",
//                TextReader.PARAM_PATTERNS, new String[] { INCLUDE_PREFIX + "*.txt" },
//                TextReader.PARAM_LANGUAGE, "en");
//
//    }
    public AnalysisEngineDescription buildMapperEngine(Configuration job)
        throws ResourceInitializationException
    {
        AnalysisEngineDescription tokenizer = createEngineDescription(BreakIteratorSegmenter.class);
        AnalysisEngineDescription stemmer = createEngineDescription(SnowballStemmer.class,
                SnowballStemmer.PARAM_LANGUAGE, "en");
        AnalysisEngineDescription writer =  createEngineDescription(TextWriter.class,
        		TextWriter.PARAM_TARGET_LOCATION,"$dir/");
//       	AnalysisEngineDescription nameFinder = createEngineDescription(
//				DictionaryAnnotator.class,
//				DictionaryAnnotator.PARAM_MODEL_LOCATION, "$dictionary/names.txt",
//				DictionaryAnnotator.PARAM_ANNOTATION_TYPE, Name.class);
        return createEngineDescription(tokenizer, stemmer,writer);

    }  
    public static void main(String[] args) throws Exception
    {
            CasConsumerExample pipeline = new CasConsumerExample();
            pipeline.setMapperClass(DkproMapper.class);
            pipeline.setReducerClass(DkproReducer.class);
         
            ToolRunner.run(new Configuration(), pipeline, args);
    }
	@Override
	public void configure(JobConf job) {
		job.set("mapreduce.job.queuename", "smalljob");	
		job.setInputFormat(Text2CASInputFormat.class);
	}
	
	@Override
	public Class getInputFormatClass() {
		// TODO Auto-generated method stub
		return null;
	}

}
