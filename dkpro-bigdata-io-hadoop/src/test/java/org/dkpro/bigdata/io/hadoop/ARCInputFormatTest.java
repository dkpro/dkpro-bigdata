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
package org.dkpro.bigdata.io.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.dkpro.bigdata.io.hadoop.ARCInputFormat.ARCRecordReader;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for webcorpus.common.io.ARCInputFormat
 * 
 * @author Johannes Simon
 *
 */
public class ARCInputFormatTest extends InputFormatTest {
	
	JobConf jobConf;
	
	final static String ARCHIVE_SIMPLE = "src/test/resources/arc/simple-archive.arc";
	final static String ARCHIVE_SEMI_COMPLEX = "src/test/resources/arc/semi-complex-archive.arc";
	
	@Before
	public void init() {
		jobConf = new JobConf(ARCInputFormatTest.class);
	}
	
	final static int offsetInRecord1 = 10;
	final static int offsetInRecord2 = 200;
	
	@Test
	public void testReadFromOffset1() throws IOException {
		Path filePath = new Path(ARCHIVE_SIMPLE);
		FileSplit inputSplit = new FileSplit(filePath, offsetInRecord1, 639, (String[])null);
		
		ARCRecordReader recordReader = new ARCRecordReader(inputSplit, jobConf);
		checkNRecordsRemaining(recordReader, 2);
	}
	
	@Test
	public void testReadFromOffset2() throws IOException {
		Path filePath = new Path(ARCHIVE_SIMPLE);
		FileSplit inputSplit = new FileSplit(filePath, offsetInRecord2, 639, (String[])null);
		
		ARCRecordReader recordReader = new ARCRecordReader(inputSplit, jobConf);
		checkNRecordsRemaining(recordReader, 1);
	}
	
	@Test
	public void testReadSimpleArchive() throws IOException {
		FileSplit inputSplit = new FileSplit(new Path(ARCHIVE_SIMPLE), 0, 639, (String[])null);
		ARCRecordReader recordReader = new ARCRecordReader(inputSplit, jobConf);
		checkNRecordsRemaining(recordReader, 2);
	}
	
	@Test
	public void testReadSemiComplexArchive() throws IOException {
		FileSplit inputSplit = new FileSplit(new Path(ARCHIVE_SEMI_COMPLEX), 0, 2382, (String[])null);
		ARCRecordReader recordReader = new ARCRecordReader(inputSplit, jobConf);
		checkNRecordsRemaining(recordReader, 9);
	}
}
