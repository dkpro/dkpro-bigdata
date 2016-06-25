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
package org.dkpro.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;
import org.dkpro.bigdata.io.hadoop.CASWritable;
import org.dkpro.bigdata.io.hadoop.MultiLineText2CASInputFormat;
import org.dkpro.bigdata.io.hadoop.MultiLineText2CASInputFormat.DocumentTextExtractor;

/**
 * Annotates output of SentenceExtractCompactJob with UIMA annotators.
 * 
 * @author Johannes Simon
 * 
 */
public class XMLDescriptorRunner extends DkproHadoopDriver {

	public static void main(String[] args) throws Exception {
		
		
		XMLDescriptorRunner job = new XMLDescriptorRunner();
		Configuration conf = new Configuration();
//		conf.set("fs.default.name", "file:///");

		int res = ToolRunner.run(conf, job, args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Usage: XMLDescriptorRunner [hadoop-params] <input> <output> <xml-descriptor-file>");
			return 1;
		}
		String descriptorPath = args[2];
		Path p = new Path(descriptorPath);
		FileSystem fs = FileSystem.get(getConf());
		if (!fs.exists(p)) {
			System.out.println("Error: Specified xml descriptor file does not exist in HDFS: " + descriptorPath);
			return 1;
		}
		getConf().set("org.jobimtext.hadoop.uima.descriptor", descriptorPath);
		return super.run(args);
	}

	@Override
	public void configure(JobConf job) {
		MultiLineText2CASInputFormat.setDocumentTextExtractorClass(job, SimpleLineInputFormat.class);
	}

	@Override
	public AnalysisEngineDescription buildMapperEngine(Configuration conf)
			throws ResourceInitializationException {
		String descriptorPath = conf.get("org.jobimtext.hadoop.uima.descriptor");
		System.err.println("Loading Analysis Engine: " + descriptorPath);
		Path p = new Path(descriptorPath);
		AnalysisEngineDescription desc = null;
		try {
			FileSystem fs = FileSystem.get(conf);
			XMLInputSource input = new XMLInputSource(fs.open(p), null);
			desc = UIMAFramework.getXMLParser()
					.parseAnalysisEngineDescription(input);
		} catch (InvalidXMLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return desc;
	}

	@Override
	public AnalysisEngineDescription buildReducerEngine(Configuration job)
			throws ResourceInitializationException {
		return null;
	}

	@Override
	public Class<? extends InputFormat<Text, CASWritable>> getInputFormatClass() {
		return MultiLineText2CASInputFormat.class;
	}

	public static class SimpleLineInputFormat implements DocumentTextExtractor {
		@Override
		public Text extractDocumentText(Text key, Text value) {
			return value;
		}
	}
}
