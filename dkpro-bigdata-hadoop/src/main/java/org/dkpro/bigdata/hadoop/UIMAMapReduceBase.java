/*******************************************************************************
 * Copyright 2013 Ubiquitous Knowledge Processing (UKP) Lab Technische Universität Darmstadt
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 ******************************************************************************/
package org.dkpro.bigdata.hadoop;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngine;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.uima.resource.ResourceInitializationException;
import org.dkpro.bigdata.io.hadoop.CASWritable;

public abstract class UIMAMapReduceBase extends MapReduceBase {
	protected Class<?> outputValueClass;
	protected Class<?> mapOutputValueClass;

	private final Log sLogger = LogFactory.getLog(getClass());
	protected AnalysisEngine engine;
	private FileSystem fs;
	private LocalFileSystem localFS;
	private Path working_dir;
	private Path results_dir;
	// inputName is either the folder name on HDFS, e.g. "news10M",
	// or a specific file, e.g. "news10M.txt"
	private String inputName;
	private String taskId;
	protected JobConf job;
	private Map<String, URL> resourceURIs;
	protected int failures = 0;
	protected int maxFailures = 100;
	protected int samplingPropability = 100;
	protected CASWritable outValue;

	public UIMAMapReduceBase() {
		super();
	}

	abstract AnalysisEngineDescription getEngineDescription(
			EngineFactory factory, JobConf job)
			throws ResourceInitializationException;

	@Override
	public void configure(JobConf job) {
		try {
			this.job = job;
			this.inputName = job.get("mapred.input.dir");
			this.taskId = job.get("mapred.task.id");
			this.mapOutputValueClass = job.getMapOutputValueClass();
			this.outputValueClass = job.getOutputValueClass();
			this.samplingPropability = job.getInt("dkpro.map.samplingratio",
					100);
			final EngineFactory engineFactory = (EngineFactory) Class.forName(
					job.get("dkpro.uima.factory",
							DkproHadoopDriver.class.getName())).newInstance();
			engineFactory.configure(job);

			final AnalysisEngineDescription engineDescription = getEngineDescription(
					engineFactory, job);

			// replace the $dir variable within the configuration.
			this.fs = FileSystem.get(job);
			this.localFS = FileSystem.getLocal(job);
			if (job.getBoolean("dkpro.output.onedirpertask", true)) {
				this.working_dir = new Path("uima_output_" +
						job.get("mapred.task.id"));
			} else {
				this.working_dir = new Path("uima_output");
			}
			final Path outputPath = FileOutputFormat.getOutputPath(job);
			this.results_dir = this.fs.startLocalOutput(outputPath,
					job.getLocalPath(this.working_dir.getName()));
			this.localFS.mkdirs(this.results_dir);
			final String[] resources = job.get("dkpro.resources", "")
					.split(",");
			sLogger.info("Writing local data to: " + this.results_dir);
			this.resourceURIs = new TreeMap<String, URL>();
			for (final String resource : resources) {
				final URL r = job.getResource(resource);
				if (r != null && !resource.isEmpty()) {
					this.resourceURIs.put(resource, r);
				}

			}
			Map<String, String> variableValues = new HashMap<String, String>();
			variableValues.put("\\$dir", this.results_dir.toString());
			variableValues.put("\\$input", this.inputName);
			variableValues.put("\\$taskid", this.taskId);
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
			if (cacheFiles != null) {
				for (Path cacheFile : cacheFiles) {
					variableValues.put("^\\$cache/" + cacheFile.getName(),
									   cacheFile.toUri().getPath());
				}
			}
			for (final Entry<String, URL> resource : this.resourceURIs.entrySet()) {
				variableValues.put("\\$" + resource,
						           resource.getValue().toString());
			}
			AnalysisEngineUtil.replaceVariables(engineDescription, variableValues);
			this.engine = createEngine(engineDescription);

		} catch (final Exception e) {
			sLogger.fatal("Error while configuring pipeline", e);
			e.printStackTrace();
			throw new RuntimeException(e);
		}

	}

	@Override
	public void close() throws IOException {
		try {
			// notify uima of the end of this collection
			this.engine.batchProcessComplete();
			this.engine.collectionProcessComplete();
			// copy back data
			copyDir(this.results_dir,
					FileOutputFormat.getWorkOutputPath(this.job));
		} catch (final AnalysisEngineProcessException e) {
			throw new IOException(e);
		}
		this.engine.destroy();
	}

	/**
	 * copy a whole directory tree from the local directory on the node back to
	 * a directory on hdfs
	 * 
	 * @param results_dir
	 * @param dest
	 * @throws IOException
	 */
	private void copyDir(Path results_dir, Path dest) throws IOException {
//		System.out.println("Copying stuff from " + results_dir + " to " + dest);
		// Copy output only if not empty
		if (this.localFS.exists(results_dir)
				&& this.localFS.listStatus(results_dir).length > 0) {
			FileSystem.get(this.job).mkdirs(dest);
			// copy the whole directory tree
			FileUtil.copy(this.localFS, results_dir, FileSystem.get(this.job),
					dest, true, this.job);
		}
	}
}
