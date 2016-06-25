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
import org.dkpro.bigdata.io.hadoop.WARCInputFormat.WARCRecordReader;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for webcorpus.common.io.ARCInputFormat
 * 
 * @author Johannes Simon
 *
 */
public class WARCInputFormatTest extends InputFormatTest {
	
	JobConf jobConf;
	
	final static String ARCHIVE_SIMPLE = "src/test/resources/warc/simple-archive.warc";
	
	@Before
	public void init() {
		jobConf = new JobConf(WARCInputFormatTest.class);
	}
	
	@Test
	public void testReadSimpleArchive() throws IOException {
		Path filePath = new Path(ARCHIVE_SIMPLE);
		FileSplit inputSplit = new FileSplit(filePath, 0, 2177, (String[])null);
		
		WARCRecordReader recordReader = new WARCRecordReader(inputSplit, jobConf);
		// Archive contains only 2 records. The arc version block is only meta data.
		checkNRecordsRemaining(recordReader, 2);
	}
	
	final static int offsetInRecord1 = 10;
	final static int offsetInRecord2 = 792;
	
	@Test
	public void testReadFromOffset1() throws IOException {
		Path filePath = new Path(ARCHIVE_SIMPLE);
		FileSplit inputSplit = new FileSplit(filePath, offsetInRecord1, 2177, (String[])null);
		
		WARCRecordReader recordReader = new WARCRecordReader(inputSplit, jobConf);
		checkNRecordsRemaining(recordReader, 2);
	}
	
	@Test
	public void testReadFromOffset2() throws IOException {
		Path filePath = new Path(ARCHIVE_SIMPLE);
		FileSplit inputSplit = new FileSplit(filePath, offsetInRecord2, 2177, (String[])null);
		
		WARCRecordReader recordReader = new WARCRecordReader(inputSplit, jobConf);
		checkNRecordsRemaining(recordReader, 1);
	}
}
