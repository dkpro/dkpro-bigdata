/*
 *   Copyright 2012
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package de.tudarmstadt.ukp.dkpro.core.hadoop;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.fit.component.CasDumpWriter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.io.Files;

import de.tudarmstadt.ukp.dkpro.bigdata.hadoop.DkproHadoopDriver;
import de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop.Text2CASInputFormat;

/**
 *
 * @author Steffen Remus
 **/
public class CasConsumerOutputTest {

	@Test
	public void test() {
		try {
			String inputdir = CasConsumerOutputTest.class.getResource("test-input").getFile();
			String outputdir = Files.createTempDir().getAbsolutePath();
			String[] args = { inputdir, outputdir };
			System.out.println(Arrays.asList(args));
			ToolRunner.run(new Configuration(), new CasConsumerOutputPipeline(), args);

			Assert.assertTrue(new File(outputdir, "_SUCCESS").exists());
			for (File uima_output_attempt_dir : new File(outputdir).listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					return pathname.isDirectory() && pathname.getName().startsWith("uima_output_attempt");
				}
			})) {
				File output_file = new File(uima_output_attempt_dir, "cas_consumer_output.txt");
				Assert.assertTrue(output_file.exists());
				Assert.assertTrue(output_file.length() > 0);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class CasConsumerOutputPipeline extends DkproHadoopDriver {

		@Override
		public AnalysisEngineDescription buildMapperEngine(Configuration job) throws ResourceInitializationException {

			AnalysisEngineDescription wri = AnalysisEngineFactory.createEngineDescription(
					CasDumpWriter.class,
					CasDumpWriter.PARAM_OUTPUT_FILE, "$dir/cas_consumer_output.txt");

			return createEngineDescription(wri);

		}

		@Override
		public AnalysisEngineDescription buildReducerEngine(Configuration job) throws ResourceInitializationException {
			return null;
		}

		@Override
		public Class<?> getInputFormatClass() {
			return Text2CASInputFormat.class;
		}

		@Override
		public void configure(JobConf job) {
			job.setOutputFormat(NullOutputFormat.class);
		}

	}

}
