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

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Generic version of LineRecordReader from org.apache.hadoop.mapred package.<br/>
 * <br/>
 * Use this class if you need to read plain-text lines and emit non-text
 * values based on the lines. This reader reads all lines in one input split in one
 * aggregated Text value.
 * <code>convertValue()</code> needs to be implemented by your subclass.<br/>
 * <br/>
 * Useful for e.g. generating UIMA CASes from plain text. In this case, the key would be
 * the offset of the input split, and the value a CAS initialized with all lines in the input split.
 * 
 * @author Johannes Simon
 */
public abstract class GenericMultiLineRecordReader<V> implements RecordReader<Text, V> {

	private final LineRecordReader lineReader;
	private final FileSplit split;
	
	public GenericMultiLineRecordReader(FileSplit split, JobConf jobConf, Reporter reporter) throws IOException {
		lineReader = new LineRecordReader(jobConf, split);
		this.split = split;
	}
	
	@Override
	public boolean next(Text key, V value) throws IOException {
		LongWritable lineKey = lineReader.createKey();
		Text docKey = new Text(split.toString());
		StringBuilder doc = new StringBuilder();
		Text line = lineReader.createValue();
		boolean success;
		while ((success = lineReader.next(lineKey, line))) {
			// Document key is key of first line (in this split)
			doc.append(line);
			doc.append("\n");
		}
		
		// success == true iff. we read at least one line
		success = doc.length() > 0;
		if (success) {
			Text docValue = new Text(doc.toString());
			convertValue(docKey, docValue, value);
		}
		
		return success;
	}

	// The following methods only delegate functionality to lineReader
	
	protected abstract void convertValue(Text longKey, Text textValue, V value);

	@Override public void close() throws IOException { lineReader.close(); }
	@Override public long getPos() throws IOException { return lineReader.getPos(); }
	@Override public float getProgress() throws IOException { return lineReader.getProgress(); }
	
}