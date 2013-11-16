/*******************************************************************************
 * Copyright 2013 Ubiquitous Knowledge Processing (UKP) Lab Technische Universität Darmstadt
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 ******************************************************************************/
package de.tudarmstadt.ukp.dkpro.bigdata.hadoop;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngine;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.analysis_engine.metadata.AnalysisEngineMetaData;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.ConfigurationParameter;
import org.apache.uima.resource.metadata.ConfigurationParameterDeclarations;
import org.apache.uima.resource.metadata.ConfigurationParameterSettings;
import org.apache.uima.util.InvalidXMLException;

import de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop.CASWritable;

public abstract class UIMAMapReduceBase extends MapReduceBase {
	protected Class<?> outputValueClass;
	protected Class<?> mapOutputValueClass;

	private final Log sLogger = LogFactory.getLog(getClass());
	protected AnalysisEngine engine;
	private FileSystem fs;
	private LocalFileSystem localFS;
	private Path working_dir;
	private Path results_dir;
	private JobConf job;
	private Map<String, URL> resourceURIs;
	protected int failures = 0;
	protected int maxFailures = 100;
	protected int samplingPropability = 100;
	protected CASWritable outValue;
	public UIMAMapReduceBase() {
		super();
	}

	abstract AnalysisEngineDescription getEngineDescription(EngineFactory factory, JobConf job) throws ResourceInitializationException;

	@Override
	public void configure(JobConf job) {
		try {
			this.job = job;
			this.mapOutputValueClass = job.getMapOutputValueClass();
			this.outputValueClass = job.getOutputValueClass();
			this.samplingPropability = job.getInt("dkpro.map.samplingratio", 100);
			final EngineFactory engineFactory = (EngineFactory) Class.forName(job.get("dkpro.uima.factory", DkproHadoopDriver.class.getName())).newInstance();
			engineFactory.configure(job);

			final AnalysisEngineDescription engineDescription = getEngineDescription(engineFactory, job);

			// replace the $dir variable within the configuration.
			this.fs = FileSystem.get(job);
			this.localFS = FileSystem.getLocal(job);
			this.working_dir = new Path("uima_output_" + job.get("mapred.task.id"));
			final Path outputPath = FileOutputFormat.getOutputPath(job);
			this.results_dir = this.fs.startLocalOutput(outputPath,
					job.getLocalPath(this.working_dir.getName()));
			this.localFS.mkdirs(this.results_dir);
			final String[] resources = job.get("dkpro.resources", "").split(",");
			System.err.println("Writing local data to: " + this.results_dir);
			this.resourceURIs = new TreeMap<String, URL>();
			for (final String resource : resources) {
				final URL r = job.getResource(resource);
				if (r != null && !resource.isEmpty()) {
					this.resourceURIs.put(resource, r);
				}

			}
			replaceRecursively(engineDescription);
			this.engine = createEngine(engineDescription);

		}
		catch (final Exception e) {
			sLogger.fatal("Error while configuring pipeline", e);
			e.printStackTrace();
			throw new RuntimeException(e);
		}

	}

	/**
	 * Search for primitive engineDescriptions and replace some variables in parameters.
	 * 
	 * Search also in nested engineDescriptions
	 * 
	 * @param engineDescription
	 * @throws InvalidXMLException
	 */
	private void replaceRecursively(AnalysisEngineDescription engineDescription) throws InvalidXMLException {
		AnalysisEngineMetaData analysisEngineMetaData = engineDescription.getAnalysisEngineMetaData();
		ConfigurationParameterDeclarations configurationParameterDeclarations = analysisEngineMetaData.getConfigurationParameterDeclarations();

		if (engineDescription.isPrimitive()) { // anchor
			replaceVariables(analysisEngineMetaData, configurationParameterDeclarations);
			return;
		}

		for (final Entry<String, ResourceSpecifier> e : engineDescription.getDelegateAnalysisEngineSpecifiers().entrySet())
			replaceRecursively((AnalysisEngineDescription) e.getValue());
	}

	/**
	 * Replace variables in the UIMA-Engines configuration to use the resources provides by hadoop.
	 * 
	 * @param analysisEngineMetaData
	 * @param configurationParameterDeclarations
	 */
	private void replaceVariables(AnalysisEngineMetaData analysisEngineMetaData, ConfigurationParameterDeclarations configurationParameterDeclarations) {
		for (final ConfigurationParameter parameter : configurationParameterDeclarations.getConfigurationParameters()) {
			final ConfigurationParameterSettings configurationParameterSettings = analysisEngineMetaData.getConfigurationParameterSettings();
			final Object parameterValue = configurationParameterSettings.getParameterValue(parameter.getName());
			if (parameterValue instanceof String) {
				/*
				 * replace $dir with the local path
				 */

				configurationParameterSettings.setParameterValue(
						parameter.getName(),
						((String) parameterValue).replaceAll("\\$dir", this.results_dir.toString()));
				/*
				 * replace $resource with the resource that has been added by addArchive.
				 */
				for (final Entry<String, URL> resource : this.resourceURIs.entrySet()) {
					if (((String) parameterValue).contains("$" + resource.getKey())) {
						System.out.println(
								"replaced $" +
										resource.getKey() +
										" in " +
										analysisEngineMetaData.getName());
					}
					configurationParameterSettings.setParameterValue(
							parameter.getName(),
							((String) parameterValue).replaceAll("\\$" + resource, resource.getValue().toString()));
				}
			}
		}
	}

	@Override
	public void close() throws IOException {
		try {
			// notify uima of the end of this collection
			this.engine.batchProcessComplete();
			this.engine.collectionProcessComplete();
			// copy back data
			copyRecursively(this.results_dir, FileOutputFormat.getWorkOutputPath(this.job));
		}
		catch (final AnalysisEngineProcessException e) {
			throw new IOException(e);
		}
		this.engine.destroy();
	}

	/**
	 * copy a whole directory tree from the local directory on the node back to a directory on hdfs
	 * 
	 * @param results_dir
	 * @param dest
	 * @throws IOException
	 */
	private void copyRecursively(Path results_dir, Path dest) throws IOException
	{
		// Copy output only if not empty
		if (this.localFS.exists(results_dir) &&
				this.localFS.listFiles(results_dir, false).hasNext()) {
			FileSystem.get(this.job).mkdirs(dest);
			// final FileStatus[] content = this.localFS.listStatus(results_dir.makeQualified(this.localFS));
			// for (final FileStatus fileStatus : content) {
			// if (fileStatus.isDirectory()) {
			// copyRecursively(fileStatus.getPath(), dest.suffix(fileStatus.getPath().getName()));
			// }
			// else {
			// FileUtil.copy(
			// this.localFS,
			// fileStatus.getPath(),
			// FileSystem.get(this.job),
			// dest.suffix(fileStatus.getPath().getName()),
			// true,
			// this.job); // !!! this deletes the files the source folder
			// }
			// }
			FileUtil.copy(this.localFS, results_dir, FileSystem.get(this.job), dest, true, this.job); // !!! this overwrites the destination folder with the source folder which was emptied before! It also copies the whole directory structure so a manual recursive copying is basically not needed.
		}
	}
}
