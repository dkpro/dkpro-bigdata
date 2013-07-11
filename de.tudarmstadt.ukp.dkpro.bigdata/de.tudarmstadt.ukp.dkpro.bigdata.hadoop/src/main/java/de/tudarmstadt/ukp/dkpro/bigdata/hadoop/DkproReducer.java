/*******************************************************************************
 * Copyright 2012
 * TU Darmstadt, FG Sprachtechnologie
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

import static org.uimafit.factory.AnalysisEngineFactory.createAggregate;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.analysis_engine.metadata.AnalysisEngineMetaData;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.impl.XCASDeserializer;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.ConfigurationParameter;
import org.apache.uima.resource.metadata.ConfigurationParameterDeclarations;
import org.apache.uima.resource.metadata.ConfigurationParameterSettings;
import org.apache.uima.util.ProcessTrace;
import org.apache.uima.util.ProcessTraceEvent;
import org.apache.uima.util.XMLParser;
import org.xml.sax.SAXException;

/**
 * This class expects a UIMA Consumer as engine, it will not collect the output but will copy
 * everything from the local directory to HDFS after completion. Useful for e.g. LuceneIndexWriter
 * 
 * @author zorn
 * 
 */
public class DkproReducer
    implements Reducer<Writable, Text, Writable, Text>
{

    AnalysisEngine engine;
    FileSystem fs;
    FileSystem localFS;
    private Path working_dir;
    private Path results_dir;
    JobConf job;
    Log logger = LogFactory.getLog("DkproReducer");
    private int casType;
    private Map<String, URL> resourceURIs;

    @Override
    public void configure(JobConf job)
    {
        try {
            this.job = job;

            XMLParser parser = UIMAFramework.getXMLParser();
            casType = job.getInt("uima.input.cas.serialization", 0);

            EngineFactory engineFactory = (EngineFactory) Class.forName(
                    job.get("dkpro.uima.factory", DkproHadoopDriver.class.getName()))
                    .newInstance();

            AnalysisEngineDescription engineDescription = engineFactory.buildReducerEngine(job);

            AnalysisEngineMetaData analysisEngineMetaData = engineDescription
                    .getAnalysisEngineMetaData();
            ConfigurationParameterDeclarations configurationParameterDeclarations = analysisEngineMetaData
                    .getConfigurationParameterDeclarations();
            // replace the $dir variable within the configuration.
            fs = FileSystem.get(job);
            localFS = FileSystem.getLocal(job);
            working_dir = new Path("uima_output_" + job.get("mapred.task.id"));
            Path outputPath = FileOutputFormat.getOutputPath(job);
            results_dir = fs.startLocalOutput(outputPath, job.getLocalPath(working_dir.getName()));
            localFS.mkdirs(results_dir);
            String[] resources = job.get("dkpro.resources", "").split(",");
            resourceURIs = new TreeMap<String, URL>();
            for (String resource : resources) {
                URL r = job.getResource(resource);
                if (r != null && !resource.isEmpty()) {
                    resourceURIs.put(resource, r);
                    System.out.println(resource + " " + r.toString());

                }
            }
            if (engineDescription.isPrimitive())
                replaceVariables(analysisEngineMetaData, configurationParameterDeclarations);
            else
                for (Entry<String, ResourceSpecifier> e : engineDescription
                        .getDelegateAnalysisEngineSpecifiers().entrySet()) {
                    analysisEngineMetaData = ((AnalysisEngineDescription) e.getValue())
                            .getAnalysisEngineMetaData();
                    configurationParameterDeclarations = analysisEngineMetaData
                            .getConfigurationParameterDeclarations();
                    replaceVariables(analysisEngineMetaData, configurationParameterDeclarations);

                }

            engine = createAggregate(engineDescription);

        }
        catch (Exception e) {
            logger.fatal("Error while configuring pipeline", e);
            e.printStackTrace();
            throw new RuntimeException();
        }

    }

    public void replaceVariables(AnalysisEngineMetaData analysisEngineMetaData,
            ConfigurationParameterDeclarations configurationParameterDeclarations)
    {
        for (ConfigurationParameter parameter : configurationParameterDeclarations
                .getConfigurationParameters()) {
            ConfigurationParameterSettings configurationParameterSettings = analysisEngineMetaData
                    .getConfigurationParameterSettings();
            Object parameterValue = configurationParameterSettings.getParameterValue(parameter
                    .getName());

            if (parameterValue instanceof String) {
                if (((String) parameterValue).contains("$dir"))
                    System.out.println("replaced $dir in " + analysisEngineMetaData.getName()
                            + " by " + results_dir);
                configurationParameterSettings.setParameterValue(parameter.getName(),
                        ((String) parameterValue).replaceAll("\\$dir", results_dir.toString()));
                for (Entry<String, URL> resource : resourceURIs.entrySet()) {
                    if (((String) parameterValue).contains("$" + resource.getKey()))
                        System.out.println("replaced $" + resource.getKey() + " in "
                                + analysisEngineMetaData.getName());
                    configurationParameterSettings.setParameterValue(parameter.getName(),
                            ((String) parameterValue).replaceAll("\\$" + resource, resource
                                    .getValue().toString()));

                }
            }
        }
    };

    @Override
    public void close()
        throws IOException
    {
        try {
            engine.batchProcessComplete();
            engine.collectionProcessComplete();
            // if(!results_dir.getFileSystem(job).equals(FileOutputFormat.getWorkOutputPath(job).getFileSystem(job)))
            // {
            System.out.println("copying from " + results_dir + " to "
                    + FileOutputFormat.getOutputPath(job));
            copyRecursively(results_dir, FileOutputFormat.getWorkOutputPath(job));
            // }
            // else
            // fs.completeLocalOutput(FileOutputFormat.getWorkOutputPath(job), results_dir);
            //
        }
        catch (AnalysisEngineProcessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        engine.destroy();
    };

    public void copyRecursively(Path results_dir, Path dest)
        throws IOException
    {

        FileUtil.copy(localFS, results_dir, FileSystem.get(job), dest, true, job);
        FileStatus[] content = localFS.listStatus(results_dir.makeQualified(localFS));
        for (FileStatus fileStatus : content) {
            if (fileStatus.isDir())
                copyRecursively(fileStatus.getPath(), dest.suffix(fileStatus.getPath().getName()));
            else
                FileUtil.copy(localFS, fileStatus.getPath(), FileSystem.get(job),
                        dest.suffix(fileStatus.getPath().getName()), true, job);
        }
    };

    @Override
    public void reduce(Writable key, Iterator<Text> values, OutputCollector<Writable, Text> output,
            Reporter reporter)
        throws IOException
    {

        while (values.hasNext())
            try {
                Text value = values.next();
                CAS aCAS = engine.newCAS();

                XCASDeserializer.deserialize(new ByteArrayInputStream(value.toString().getBytes("UTF-8")), aCAS);
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