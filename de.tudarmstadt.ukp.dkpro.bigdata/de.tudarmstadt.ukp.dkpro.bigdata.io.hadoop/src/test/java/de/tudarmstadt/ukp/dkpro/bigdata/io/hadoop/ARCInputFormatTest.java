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
package de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop.ARCInputFormat;
import de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop.ARCInputFormat.ARCRecordReader;

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
	/*
	@Test
	public void testArchiveValidity() throws IOException {
		Path filePath = new Path(ARCHIVE_SIMPLE);
		FileSplit inputSplit = new FileSplit(filePath, 10, 1096, (String[])null);
		
		ARCRecordReader recordReader = new ARCRecordReader(inputSplit, jobConf);
		recordReader.validate();
	}*/
	
	@Test
	public void testReadFromStart() throws IOException {
		Path filePath = new Path(ARCHIVE_SIMPLE);
		FileSplit inputSplit = new FileSplit(filePath, 0, 1096, (String[])null);
		
		ARCRecordReader recordReader = new ARCRecordReader(inputSplit, jobConf);
		// Archive contains only 2 records. The arc version block is only meta data.
		checkNRecordsRemaining(recordReader, 2);
	}
	
	@Test
	public void testReadParts() throws IOException {
		Path filePath = new Path(ARCHIVE_SIMPLE);
		FileSplit inputSplit1 = new FileSplit(filePath, 0, 1, (String[])null);
		FileSplit inputSplit2 = new FileSplit(filePath, 1, 200, (String[])null);
		FileSplit inputSplit3 = new FileSplit(filePath, 200, 639, (String[])null);
		
		ARCRecordReader recordReader1 = new ARCRecordReader(inputSplit1, jobConf);
		ARCRecordReader recordReader2 = new ARCRecordReader(inputSplit2, jobConf);
		ARCRecordReader recordReader3 = new ARCRecordReader(inputSplit3, jobConf);
		
		// Archive contains only 2 records. The arc version block is only meta data.
		checkNRecordsRemaining(recordReader1, 0);
		checkNRecordsRemaining(recordReader2, 1);
		checkNRecordsRemaining(recordReader3, 1);
	}
	
	final static int offsetInRecord1 = 10;
	final static int offsetInRecord2 = 200;
	
	@Test
	public void testReadFromOffset1() throws IOException {
		Path filePath = new Path(ARCHIVE_SIMPLE);
		FileSplit inputSplit = new FileSplit(filePath, offsetInRecord1, 1096, (String[])null);
		
		ARCRecordReader recordReader = new ARCRecordReader(inputSplit, jobConf);
		checkNRecordsRemaining(recordReader, 2);
	}
	
	@Test
	public void testReadFromOffset2() throws IOException {
		Path filePath = new Path(ARCHIVE_SIMPLE);
		FileSplit inputSplit = new FileSplit(filePath, offsetInRecord2, 1096, (String[])null);
		
		ARCRecordReader recordReader = new ARCRecordReader(inputSplit, jobConf);
		checkNRecordsRemaining(recordReader, 1);
	}
	
	@Test
	public void testReadSimpleArchiveInSplits() throws IOException {
		ARCInputFormat inputFormat = new ARCInputFormat();
		assertEquals(2, readArchiveInSplits(ARCHIVE_SIMPLE, Integer.MAX_VALUE, inputFormat, jobConf));
		assertEquals(2, readArchiveInSplits(ARCHIVE_SIMPLE, 1000, inputFormat, jobConf));
		assertEquals(2, readArchiveInSplits(ARCHIVE_SIMPLE, 100, inputFormat, jobConf));
		assertEquals(2, readArchiveInSplits(ARCHIVE_SIMPLE, 10, inputFormat, jobConf));
	}
	
	@Test
	public void testReadSemiComplexArchiveInSplits() throws IOException {
		ARCInputFormat inputFormat = new ARCInputFormat();
		assertEquals(9, readArchiveInSplits(ARCHIVE_SEMI_COMPLEX, Integer.MAX_VALUE, inputFormat, jobConf));
		assertEquals(9, readArchiveInSplits(ARCHIVE_SEMI_COMPLEX, 1000, inputFormat, jobConf));
		assertEquals(9, readArchiveInSplits(ARCHIVE_SEMI_COMPLEX, 100, inputFormat, jobConf));
		assertEquals(9, readArchiveInSplits(ARCHIVE_SEMI_COMPLEX, 10, inputFormat, jobConf));
	}
}
