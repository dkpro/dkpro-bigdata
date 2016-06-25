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
package org.dkpro.bigdata.hadoop;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;
import static org.apache.uima.fit.factory.CollectionReaderFactory.createReader;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.resource.ResourceInitializationException;
import org.dkpro.bigdata.hadoop.DkproHadoopDriver;
import org.dkpro.bigdata.io.hadoop.CASWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import de.tudarmstadt.ukp.dkpro.core.io.text.TextReader;
import de.tudarmstadt.ukp.dkpro.core.io.text.TextWriter;
import de.tudarmstadt.ukp.dkpro.core.tokit.BreakIteratorSegmenter;

public class DkproHadoopDriverTest
{
    // Need to use this for a proper temporary folder because otherwise we get an error if
    // the tests runs within some folder that has percentage signs in its path...
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private File hadoopTmp;

    private MiniDFSCluster hdfsCluster;

    @Before
    public void startCluster()
        throws Exception
    {
        // Start dummy HDFS
        File target = folder.newFolder("hdfs");
        hadoopTmp = folder.newFolder("hadoop");

        File baseDir = new File(target, "hdfs").getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        conf.set("hadoop.tmp.dir", hadoopTmp.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
    }

    @After
    public void shutdownCluster()
    {
        hdfsCluster.shutdown();
    }

    @Test
    public void testHadoopDriver()
        throws Exception
    {
        String hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";

        final DkproHadoopDriver ab = new Driver();

        String[] args = { "testdata", "output" };

        Configuration conf = new Configuration();
        conf.set("fs.default.name", hdfsURI);
        conf.set("dfs.namenode.resource.du.reserved", "0");
        conf.set("hadoop.tmp.dir", hadoopTmp.getAbsolutePath());

        ToolRunner.run(conf, ab, args);
        
        RemoteIterator<LocatedFileStatus> files = hdfsCluster.getFileSystem()
                .listFiles(new Path("output"), true);
        
        assertTrue(files.hasNext());
        assertEquals("_SUCCESS", files.next().getPath().getName());
    }

    public static class Driver
        extends DkproHadoopDriver
    {
        @Override
        public CollectionReader buildCollectionReader()
            throws ResourceInitializationException
        {
            return createReader(TextReader.class, 
                    TextReader.PARAM_SOURCE_LOCATION, "src/test/resources/test-input/", 
                    TextReader.PARAM_PATTERNS, "[+]*.txt");
        }

        @Override
        public AnalysisEngineDescription buildMapperEngine(Configuration job)
            throws ResourceInitializationException
        {
            return createEngineDescription(
                    createEngineDescription(BreakIteratorSegmenter.class),
                    createEngineDescription(createEngineDescription(TextWriter.class,
                            TextWriter.PARAM_TARGET_LOCATION, "$dir/output")));
        }

        @Override
        public void configure(JobConf job)
        {
            // TODO Auto-generated method stub
        }

        @Override
        public Class<? extends InputFormat<Text, CASWritable>> getInputFormatClass()
        {
            return null;
        }
    }
}
