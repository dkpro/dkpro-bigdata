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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.dkpro.bigdata.io.hadoop.CrawlerRecord;

/**
 * Base class providing helper methods for InputFormat unit tests
 * 
 * @author Johannes Simon
 *
 */
public class InputFormatTest {

	public InputFormatTest() {
		super();
	}

	protected void checkRecordValidity(Text key, CrawlerRecord value) {
//		System.out.println("Found record: " + key);
		assertEquals("Keys must not be empty", key.toString().isEmpty(), false);
		assertEquals("Key must be URL of record", key.toString(), value.getURL());
	}
	
	protected void checkNRecordsRemaining(RecordReader<Text, CrawlerRecord> recordReader, int n) throws IOException {
		int numRecordsRemaining = checkRecordsRemaining(recordReader);
		assertEquals(n, numRecordsRemaining);
	}

	protected int checkRecordsRemaining(RecordReader<Text, CrawlerRecord> recordReader) throws IOException {
		int numRecordsRemaining = 0;
		while (true) {
			Text key = recordReader.createKey();
			CrawlerRecord value = recordReader.createValue();
			boolean recordRead = recordReader.next(key, value);

			// Check every record read
			if (recordRead) {
				numRecordsRemaining++;
				checkRecordValidity(key, value);
			} else {
				break;
			}
		}

		return numRecordsRemaining;
	}

	
	public int readArchiveInSplits(String archiveFile, int bytesPerSplit, InputFormat<Text, CrawlerRecord> inputFormat, JobConf job) throws IOException {
		Path filePath = new Path(archiveFile);
		File file = new File(archiveFile);
		
		int numRecordsRead = 0;
		
		System.out.println("Reading archive of size " + file.length() + " in splits of size " + bytesPerSplit);
		for (int offset = 0; offset < file.length(); offset += bytesPerSplit ) {
//			System.out.println("Read from " + offset + " to " + (offset + bytesPerSplit));
			FileSplit inputSplit = new FileSplit(filePath, offset, bytesPerSplit, (String[])null);
			RecordReader<Text, CrawlerRecord> recordReader = inputFormat.getRecordReader(inputSplit, job, Reporter.NULL);
			numRecordsRead += checkRecordsRemaining(recordReader);
		}
		
		return numRecordsRead;
	}
}