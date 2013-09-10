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

import org.junit.Ignore;
import org.junit.Test;

import de.tudarmstadt.ukp.dkpro.bigdata.hadoop.DkproHadoopDriver;

public class DkproHadoopDriverTest
{
    @Test
    @Ignore
    public void testHadoopDriver()
        throws Exception
    {
        final DkproHadoopDriver ab = new MyDriver();

        final boolean ok = true;
        // String[] args = {"testdata","output"};
        //
        // Configuration conf = new Configuration();
        // conf.set("fs.default.name","file:///");
        //
        // ToolRunner.run(conf,ab, args );
        //
        assert (ok);
    }
}
