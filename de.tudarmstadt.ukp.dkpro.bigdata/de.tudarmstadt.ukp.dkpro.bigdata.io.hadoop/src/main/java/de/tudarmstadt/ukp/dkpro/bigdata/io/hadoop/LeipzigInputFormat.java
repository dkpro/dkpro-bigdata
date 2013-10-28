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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import java.util.regex.MatchResult;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.input.CountingInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Creates LeipzigRecordReader for Leipzig corpora
 * 
 * @author Johannes Simon
 *
 */
public class LeipzigInputFormat extends FileInputFormat<Text, CrawlerRecord> {
	/**
	 * Parse and modify source metadata as given in Leipzig corpora.
	 * 
	 * @author LSW
	 * 
	 */
	public static class SourceMetadata {

		private Document doc;
		
		public SourceMetadata() {
			// Initialize with valid placeholder meta data XML
			try {
				loadXml("<source><location>null</location><date>null</date><user>null</user><original_encoding>null</original_encoding><language>null</language><issue>null</issue></source>");
			} catch (SAXException e) {
				e.printStackTrace();
			}
		}
		
		public SourceMetadata(String xml) throws SAXException {
			loadXml(xml);
		}
		
		private void loadXml(String xml) throws SAXException {

			if (!xml.contains("<location><![CDATA[")) {
				xml = xml.replace("<location>", "<location><![CDATA[");
				xml = xml.replace("</location>", "]]></location>");
			}

			try {
				DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
				DocumentBuilder db = dbf.newDocumentBuilder();
				InputSource is = new InputSource();
				is.setCharacterStream(new StringReader(xml));

				doc = db.parse(is);
			} catch (ParserConfigurationException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public String getEntry(String name) {
			try {
				NodeList nodes = doc.getElementsByTagName(name);
				Element line = (Element) nodes.item(0);
				return getCharacterDataFromElement(line);
			} catch (NullPointerException e) {
				return null;
			}
		}

		public void setEntry(String name, String entry) {
			try {
				NodeList nodes = doc.getElementsByTagName(name);
				Element item = (Element) nodes.item(0);

				Node child = item.getFirstChild();

				child.setNodeValue(entry);

			} catch (NullPointerException e) {
				System.out.println("DocumentMetadata: could not write to " + name + " - " + entry);
			}
		}

		private String getCharacterDataFromElement(Element e) {
			Node child = e.getFirstChild();
			if (child instanceof CharacterData) {
				CharacterData cd = (CharacterData) child;
				return cd.getData();
			}
			return "?";
		}

		public String getXMLString() {

			Transformer transformer;

			try {
				transformer = TransformerFactory.newInstance().newTransformer();
				transformer.setOutputProperty(OutputKeys.INDENT, "no");
				StreamResult result = new StreamResult(new StringWriter());
				DOMSource source = new DOMSource(doc);
				transformer.transform(source, result);

				String xmlString = result.getWriter().toString();

				xmlString = xmlString.replace("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>", "");

				if (!xmlString.contains("<location><![CDATA[")) {
					xmlString = xmlString.replace("<location>", "<location><![CDATA[");
					xmlString = xmlString.replace("</location>", "]]></location>");
				}

				return xmlString;
			} catch (TransformerConfigurationException e) {
				e.printStackTrace();
			} catch (TransformerFactoryConfigurationError e) {
				e.printStackTrace();
			} catch (TransformerException e) {
				e.printStackTrace();
			}

			return null;

		}

		public static void main(String[] args) {
			String data = "<source><location>http://www.bedakafi.ch/anfragen.html</location><date>2011-02-02</date><user>Treasurer</user><original_encoding>utf-8</original_encoding><language>deu</language><issue>encoding</issue></source>";

			SourceMetadata dm;
			try {
				dm = new SourceMetadata(data);

				System.out.println(dm.getEntry("location"));

				dm.setEntry("location", "http://localhost");
				System.out.println(dm.getEntry("issue"));
				System.out.println(dm.getEntry("not existent"));

				System.out.println(dm.getXMLString());
			} catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
    public RecordReader<Text, CrawlerRecord> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        return new LeipzigRecordReader((FileSplit) inputSplit, jobConf);
    }
    
    /**
     * Reads text corpus entries in Leipzig format
     * 
     * @author Johannes Simon
     *
     */
    public static class LeipzigRecordReader implements RecordReader<Text, CrawlerRecord> {
        private long start;
        private long end;
        private CountingInputStream countingIs;
        private BufferedReader reader;
        
        private long nextRecordStart;
        
    	private Scanner recordScanner;
    	private final String RECORD_DELIMITER = "<source>";
    	private String nextRecord;
    	private boolean nextRecordIsValid = false;

        private long posInByteStream;
        private long posInCharStream;
        
        private final String FILE_ENCODING = "UTF-8";

        /*
         * ======================== RecordReader Logic ============================
         */

        public LeipzigRecordReader(FileSplit split, JobConf jobConf) throws IOException {
            start = split.getStart();
            end = start + split.getLength();
            
            posInByteStream = start;
            posInCharStream = 0;

            // Open the file and seek to the start of the split
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(jobConf);
            InputStream is = fs.open(split.getPath());
            countingIs = new CountingInputStream(is);
            countingIs.skip(start);
        	recordScanner = new Scanner(countingIs);
        	recordScanner.useDelimiter(RECORD_DELIMITER);
            reader = new BufferedReader(new InputStreamReader(countingIs, FILE_ENCODING));
            // Start with the first valid record after offset "start"
            while (!nextRecordIsValid && hasNext())
            	skipToNextRecord(reader);
        }
        
        private boolean parseMetaLine(CrawlerRecord value, String line) {
        	try {
				SourceMetadata sm = new SourceMetadata(line);
				String origUrl = sm.getEntry("location");
				String url;
				if (!origUrl.isEmpty() && !origUrl.equalsIgnoreCase("null")) {
					url = origUrl;
				} else {
					// Input format is not responsible for filtering incomplete records!
					// Simply set URL to "null" (a valid string, not null!) at this point
					url = "null";
				}
				value.setURL(url);
	
				// Original encoding
				String encoding = sm.getEntry("original_encoding");
				value.setOriginalEncoding(encoding);
				
				// Date
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
				Date parsedDate = null;
				String date = sm.getEntry("date");
				if (date != null) {
					try {
						parsedDate = dateFormat.parse(date);
					} catch (ParseException e) {
						System.err.println("[LeipzigInputFormat] Warning: Can't parse date: " + date);
					}
				} else {
					System.err.println("[LeipzigInputFormat] Warning: Record is missing a date.");
				}
				value.setDate(parsedDate);
        	} catch (Exception e) {
        		System.err.println("[LeipzigInputFormat] Warning: Skipping record because an exception occured while parsing meta line " + line);
        		System.err.println("[LeipzigInputFormat] Exception details: " + e.getMessage());
        		return false;
        	}
        	
        	return true;
        }
        
		public static final String LF = System.getProperty("line.separator");
		
		private boolean hasNext() {
//			System.out.println("hasNext: " + nextRecordLine + " != null && " + nextRecordStart + " < " + end);
			return nextRecordStart >= 0 && nextRecordStart < end;
		}
		
		private String extractMetaLine(String record) {
        	int metaLineEnd = nextRecord.indexOf("\n");
			return nextRecord.substring(0, metaLineEnd);
		}
		
		private String extractContent(String record) {
        	int metaLineEnd = nextRecord.indexOf("\n");
			return nextRecord.substring(metaLineEnd);
		}

		public boolean next(Text key, CrawlerRecord value) throws IOException {
			if (!hasNext())
				return false;

			// Try parsing meta line. If parsing failed, skip to next record, and so on.
			while (!parseMetaLine(value, extractMetaLine(nextRecord))) {
				if (hasNext())
					skipToNextRecord(reader);
				else
					return false;
			}
        	value.setContent(extractContent(nextRecord));
	        key.set(value.getURL());

			skipToNextRecord(reader);
        	
        	return true;
        }

        public Text createKey() {
            return new Text();
        }

        public CrawlerRecord createValue() {
            return new CrawlerRecord();
        }

        public long getPos() throws IOException {
            //return countingIs.getCount();
        	return posInByteStream;
        }

        public void close() throws IOException {
            countingIs.close();
        }

        public float getProgress() throws IOException {
            return ((float) (getPos() - start)) / ((float) (end - start));
        }

        /*
         * ======================== ARC Logic ============================
         */
        
        /**
         * Reads from <code>input</code> until a valid record meta line was read
         */
        private boolean skipToNextRecord(BufferedReader input) throws IOException {
        	return readUntilNextRecord(input, null);
        }

        /**
         * Reads from <code>input</code> until a valid record meta line was read. Everything
         * else is added to <code>buffer</code>
         */
        private boolean readUntilNextRecord(BufferedReader input, StringBuffer buffer) throws IOException {
        	nextRecordStart = -1;
        	if (recordScanner.hasNext()) {
        		nextRecordStart = posInByteStream;
        		String record = recordScanner.next();
        		MatchResult m = recordScanner.match();
        		int recordSizeBytes = record.getBytes("UTF-8").length;

        		// Check if we skipped characters
        		int delimCharsSkipped;
        		// Scanner flushed its buffer (can be only reason that (m.start() > posInCharStream) always holds)
        		if (posInCharStream > 0 && m.start() == 0) {
        			posInCharStream = 0;
        			// Scanner refills buffer *after* delimiter
        			delimCharsSkipped = RECORD_DELIMITER.length();
        		} else {
        			delimCharsSkipped = (int) (m.start() - posInCharStream);
        		}
        		
        		if (delimCharsSkipped > RECORD_DELIMITER.length()) {
        			System.err.println("Internal error: Skipped chars other than \"" + RECORD_DELIMITER + "\"");
        		} else {
        			try {
	        		String delimSkipped = RECORD_DELIMITER.substring(RECORD_DELIMITER.length() - delimCharsSkipped);
	        		posInByteStream += delimSkipped.getBytes("UTF-8").length;
        			} catch (StringIndexOutOfBoundsException e) {
        				e.printStackTrace();
        			}
        		}
        		posInCharStream = m.end();
        		posInByteStream += recordSizeBytes;
        		nextRecord = RECORD_DELIMITER + record;
        		nextRecordIsValid = delimCharsSkipped == RECORD_DELIMITER.length();
        		
        		return true;
        	}
        	
        	return false;
        }
    }
}