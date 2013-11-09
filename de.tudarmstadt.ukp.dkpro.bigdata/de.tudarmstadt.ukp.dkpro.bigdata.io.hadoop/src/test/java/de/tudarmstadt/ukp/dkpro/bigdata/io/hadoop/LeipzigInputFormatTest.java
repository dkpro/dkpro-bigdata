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

import de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop.LeipzigInputFormat;
import de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop.LeipzigInputFormat.LeipzigRecordReader;

/**
 * Unit tests for webcorpus.common.io.LeipzigInputFormat
 * 
 * @author Johannes Simon
 *
 */
public class LeipzigInputFormatTest extends InputFormatTest {
	
	JobConf jobConf;
	
	// Test file is 364 bytes (not characters!) in size
	final static String SIMPLE_ARCHIVE = "src/test/resources/leipzig/simple-archive.leipzig";
	final static String PROBLEMATIC_ARCHIVE = "src/test/resources/leipzig/problematic-archive.leipzig";
	//final static String COMPLEX_ARCHIVE = "src/test/resources/leipzig/complex-archive.leipzig";
	final static String SEMI_COMPLEX_ARCHIVE = "src/test/resources/leipzig/semi-complex-archive.leipzig";
	//final static String LARGE_RECORD_ARCHIVE = "src/test/resources/leipzig/large-record-archive.leipzig";
	
	@Before
	public void init() {
		jobConf = new JobConf(LeipzigInputFormatTest.class);
	}
	
	@Test
	public void testReadFromStart() throws IOException {
		FileSplit inputSplit = new FileSplit(new Path(SIMPLE_ARCHIVE), 0, 364, (String[])null);
		
		LeipzigRecordReader recordReader = new LeipzigRecordReader(inputSplit, jobConf);
		checkNRecordsRemaining(recordReader, 2);
	}
	
	@Test
	public void testReadParts() throws IOException {
		// If split ends within a record (e.g. 1 byte is well within the first record)
		// then RecordReader must read until the end of the record
		FileSplit inputSplit1 = new FileSplit(new Path(SIMPLE_ARCHIVE), 0, 1, (String[])null);
		// Also, if split begins within a record then RecordReader must start at 
		// beginning of next record
		FileSplit inputSplit2 = new FileSplit(new Path(SIMPLE_ARCHIVE), 1, 364, (String[])null);
		
		LeipzigRecordReader recordReader1 = new LeipzigRecordReader(inputSplit1, jobConf);
		LeipzigRecordReader recordReader2 = new LeipzigRecordReader(inputSplit2, jobConf);
		checkNRecordsRemaining(recordReader1, 1);
		checkNRecordsRemaining(recordReader2, 1);
	}
	
	@Test
	public void testReadEmptyPart() throws IOException {
		// If split starts and ends within a record, than no record should be read
		FileSplit inputSplit = new FileSplit(new Path(SIMPLE_ARCHIVE), 1, 1, (String[])null);
		
		LeipzigRecordReader recordReader = new LeipzigRecordReader(inputSplit, jobConf);
		checkNRecordsRemaining(recordReader, 0);
	}
	
	final static int offsetInRecord1 = 10;
	final static int offsetInRecord2 = 200;
	
	@Test
	public void testReadFromOffset1() throws IOException {
		FileSplit inputSplit = new FileSplit(new Path(SIMPLE_ARCHIVE), offsetInRecord1, 364, (String[])null);
		
		LeipzigRecordReader recordReader = new LeipzigRecordReader(inputSplit, jobConf);
		checkNRecordsRemaining(recordReader, 1);
	}
	
	@Test
	public void testReadFromOffset2() throws IOException {
		FileSplit inputSplit = new FileSplit(new Path(SIMPLE_ARCHIVE), offsetInRecord2, 364, (String[])null);
		
		LeipzigRecordReader recordReader = new LeipzigRecordReader(inputSplit, jobConf);
		checkNRecordsRemaining(recordReader, 0);
	}
	
	@Test
	public void testReadProblematicArchive() throws IOException {
		Path filePath = new Path(PROBLEMATIC_ARCHIVE);
		FileSplit inputSplit = new FileSplit(filePath, 0, Integer.MAX_VALUE, (String[])null);
		
		LeipzigRecordReader recordReader = new LeipzigRecordReader(inputSplit, jobConf);
		checkNRecordsRemaining(recordReader, 9);
	}

	/*
	@Test
	public void testReadComplexArchive() throws IOException {
		Path filePath = new Path(COMPLEX_ARCHIVE);
		FileSplit inputSplit = new FileSplit(filePath, 0, Integer.MAX_VALUE, (String[])null);
		
		LeipzigRecordReader recordReader = new LeipzigRecordReader(inputSplit, jobConf);
		checkNRecordsRemaining(recordReader, 438);
	}
	*/
	
	@Test
	public void testReadSimpleArchiveInSplits() throws IOException {
		LeipzigInputFormat inputFormat = new LeipzigInputFormat();
		String archiveFile = SIMPLE_ARCHIVE;
		assertEquals(2, readArchiveInSplits(archiveFile, 10, inputFormat, jobConf));
		assertEquals(2, readArchiveInSplits(archiveFile, 1024 * 1, inputFormat, jobConf));
		assertEquals(2, readArchiveInSplits(archiveFile, 1024 * 10, inputFormat, jobConf));
		assertEquals(2, readArchiveInSplits(archiveFile, 1024 * 100, inputFormat, jobConf));
		assertEquals(2, readArchiveInSplits(archiveFile, 1024 * 1000, inputFormat, jobConf));
	}
	
	@Test
	public void testReadSemiComplexArchive() throws IOException {
		LeipzigInputFormat inputFormat = new LeipzigInputFormat();
		String archiveFile = SEMI_COMPLEX_ARCHIVE;
		assertEquals(5, readArchiveInSplits(archiveFile, 1024 * 1000, inputFormat, jobConf));
		assertEquals(5, readArchiveInSplits(archiveFile, 1024 * 100, inputFormat, jobConf));
		assertEquals(5, readArchiveInSplits(archiveFile, 1024 * 10, inputFormat, jobConf));
		assertEquals(5, readArchiveInSplits(archiveFile, Integer.MAX_VALUE, inputFormat, jobConf));
	}
	
	@Test
	public void testReadSemiComplexArchiveInSplits() throws IOException {
		LeipzigInputFormat inputFormat = new LeipzigInputFormat();
		String archiveFile = SEMI_COMPLEX_ARCHIVE;
		assertEquals(5, readArchiveInSplits(archiveFile, 1024 * 1000, inputFormat, jobConf));
		assertEquals(5, readArchiveInSplits(archiveFile, 1024 * 100, inputFormat, jobConf));
		assertEquals(5, readArchiveInSplits(archiveFile, 1024 * 10, inputFormat, jobConf));
		assertEquals(5, readArchiveInSplits(archiveFile, 10, inputFormat, jobConf));
	}
	
	/*
	@Test
	public void testReadLargeRecordArchiveInSplits() throws IOException {
		LeipzigInputFormat inputFormat = new LeipzigInputFormat();
		String archiveFile = LARGE_RECORD_ARCHIVE;
		assertEquals(2, readArchiveInSplits(archiveFile, Integer.MAX_VALUE, inputFormat, jobConf));
	}
	*/
}
