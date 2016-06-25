/*******************************************************************************
 * Copyright 2015
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
package org.dkpro.bigdata.io.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.fit.factory.ExternalResourceFactory;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ExternalResourceDescription;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import de.tudarmstadt.ukp.dkpro.core.io.text.TextReader;

public class HdfsResourceLoaderLocatorTest
{
    // Need to use this for a proper temporary folder because otherwise we get an error if
    // the tests runs within some folder that has percentage signs in its path...
    @Rule
    public TemporaryFolder folder= new TemporaryFolder();
    
    private MiniDFSCluster hdfsCluster;
    
    private File hadoopTmp;

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
        hdfsCluster.waitActive();
    }
    
    @After
    public void shutdownCluster()
    {
        hdfsCluster.shutdown();
    }

    @Test
    public void testDKProResourceLoading()
        throws Exception
    {
        String hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";

        DistributedFileSystem fs = hdfsCluster.getFileSystem();
        fs.mkdirs(new Path("/user/test"));
        fs.copyFromLocalFile(new Path("src/test/resources/hdfsLocator/one.data"), 
                new Path("/user/test/"));
        fs.copyFromLocalFile(new Path("src/test/resources/hdfsLocator/two.data"), 
                new Path("/user/test/"));
        
        
        ExternalResourceDescription hdfsResource = ExternalResourceFactory
                .createExternalResourceDescription(
                        HdfsResourceLoaderLocator.class,
                        HdfsResourceLoaderLocator.PARAM_FILESYSTEM, hdfsURI);

        CollectionReader reader = CollectionReaderFactory.createReader(
                TextReader.class,
                TextReader.KEY_RESOURCE_RESOLVER, hdfsResource, 
                TextReader.PARAM_SOURCE_LOCATION, "hdfs:/user/test", 
                TextReader.PARAM_PATTERNS, "*.data");

        List<String> documents = readDocuments(reader);

        assertEquals(2, documents.size());
        assertTrue(documents.get(0).equals("Text of file one."));
        assertTrue(documents.get(1).equals("Text of file two."));

    }

    private List<String> readDocuments(CollectionReader aReader)
        throws Exception
    {
        List<String> documentContents = new ArrayList<String>();
        while (aReader.hasNext()) {
            JCas createJCas = JCasFactory.createJCas();
            aReader.getNext(createJCas.getCas());
            String text = createJCas.getDocumentText();
            documentContents.add(text);
        }
        return documentContents;
    }
}
