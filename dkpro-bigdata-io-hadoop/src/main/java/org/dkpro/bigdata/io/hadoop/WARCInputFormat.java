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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.jwat.warc.WarcConstants;
import org.jwat.warc.WarcHeader;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

//import de.uni_leipzig.asv.encodingdetector.utils.EncodingDetector;

/**
 * Creates WARCRecordReader for Crawler archives in WARC format
 * 
 * @author Johannes Simon
 * 
 */
public class WARCInputFormat
    extends FileInputFormat<Text, CrawlerRecord>
{
    @Override
    public RecordReader<Text, CrawlerRecord> getRecordReader(InputSplit inputSplit,
            JobConf jobConf, Reporter reporter)
        throws IOException
    {
        return new WARCRecordReader((FileSplit) inputSplit, jobConf);
    }

    /**
     * Reads Crawler archives in WARC format
     * 
     * @author Johannes Simon
     * 
     */
    public static class WARCRecordReader
        implements RecordReader<Text, CrawlerRecord>
    {
        private final long start;
        private final long end;
        private final CountingInputStream fsin;
        WarcReader warcReader = null;
        long lastRecordEnd = -1;
        private JobConf conf = null;

        private final Set<String> contentTypeWhitelist = new HashSet<String>();

        /*
         * ======================== RecordReader Logic ============================
         */

        /**
         * Parses and sets configuration parameters according to a JobConf/Configuration instance
         */
        void configure(Configuration conf)
        {
            String contentTypeWhitelistStr = conf.get(
                    "dkpro.input.content-type-whitelist", "text/html");
            if (contentTypeWhitelistStr != null) {
                String[] contentTypes = contentTypeWhitelistStr.replace(" ", "").split(",");
                for (String ct : contentTypes)
                    contentTypeWhitelist.add(ct);
            }
        }

        /**
         * Creates an ARCRecordReader for the specified <code>split</code> of the input stream that
         * will start at the first valid ARC record header after <code>split.getStart()</code> and
         * continue until a record is read that goes past
         * <code>split.getStart() + split.getLength()</code>.
         */
        public WARCRecordReader(FileSplit split, JobConf jobConf)
            throws IOException
        {
            conf = jobConf;
            start = split.getStart();
            end = start + split.getLength();
            System.out.println("========== " + start + " " + end);

            configure(jobConf);

            // Open the file and seek to the start of the split
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(jobConf);
            fsin = new CountingInputStream(new BufferedInputStream(fs.open(split.getPath())));

            warcReader = WarcReaderFactory.getReader(fsin);
            // Start with the first valid record after offset "start"
            skipToNextRecord(start);
        }

        private void fillCrawlerRecord(WarcRecord record, CrawlerRecord crawlerRecord)
            throws IOException
        {
            // Fill CrawlerRecord
            WarcHeader header = record.header;
            // h.warcSegmentOriginIdUrl
            crawlerRecord.setURL(header.warcTargetUriStr);
            // Usually ARC records contain the original webpage, including markup
            crawlerRecord.setIsHTML(getContentType(record).contains("text/html"));
            crawlerRecord.setDate(header.warcDate);
            // Usually not present in ARC records
            crawlerRecord.setOriginalLanguage(null);

            // It is important to *not* try to decode anything before we know the encoding.
            // As soon as we put any of this record content into a string, it is explicitly
            // converted
            // from/to some encoding. So before that, we have to guess the correct encoding,
            // otherwise
            // we'll potentially trash our data before we process it.
            byte[] buffer = IOUtils.toByteArray(record.getPayloadContent());
            Class<?> encodingDetectorClass = conf.getClass("dkpro.input.encodingdetector", DummyEncodingDetector.class);
			try {
				EncodingDetector encodingDetector = (EncodingDetector)encodingDetectorClass.newInstance();
	            // This is the encoding we'll use to decode the bytes into text
	            String encoding = encodingDetector.getBestEncoding(buffer);
	            crawlerRecord.setOriginalEncoding(encoding);
	            crawlerRecord.setContent(new String(buffer, encoding));
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }

        @Override
        public boolean next(Text key, CrawlerRecord value)
            throws IOException
        {
            WarcRecord arcRecord = null;
            boolean atEnd = false;
            long bufferMarkAtEnd = 0;

            while ((arcRecord = warcReader.getNextRecord()) != null) {
                // Check if arcReader has definitely read over end mark when considering that reader
                // is buffered
                // (meaning that (fsin.getCount() > end) is true before we've read the last record
                // before end)
                lastRecordEnd = fsin.getCount();
                if (!atEnd && lastRecordEnd >= end) {
                    atEnd = true;
                    bufferMarkAtEnd = lastRecordEnd;
                }
                else if (atEnd && lastRecordEnd > bufferMarkAtEnd) {
                    break;
                }
                try {
                    // Skip meta header (usually first record in archive)
                    if (WarcConstants.RT_WARCINFO.equals(arcRecord.header.warcTypeStr) || !arcRecord.hasPayload())
                        continue;

                    // Make sure only text content is read
                    if (conf.getBoolean("dkpro.input.filter-mimetypes",
                            false)) {

                    	// requests etc. are already filtered by mimetype but maybe in the future this needs to be revised
                    	// if (!(WarcConstants.RT_RESOURCE.equals(arcRecord.header.warcTypeStr) || WarcConstants.RT_RESPONSE.equals(arcRecord.header.warcTypeStr)))
                    	// continue;
                    	String contentTypeHeader = getContentType(arcRecord);

                        boolean skipContentType = true;
                        if (contentTypeHeader != null) {
                            for (String contentType : contentTypeWhitelist)
                                if (contentTypeHeader.contains(contentType)) {
                                    skipContentType = false;
                                    break;
                                }
                        }

                        if (skipContentType) {
                            continue;
                        }
                    }

                    fillCrawlerRecord(arcRecord, value);
                    key.set(value.getURL());

                    // System.out.println("READ");
                    return true;
                }
                catch (UnsupportedEncodingException e) {
                    // Skip unreadable records
                    System.err.println("WARNING: Skipping WARC record (byte offset "
                            + fsin.getCount()
                            + ") due to unsupported encoding. The record may contain binary data.");
                }
                catch (Exception e) {
                    // Skip any other records that produce exceptions
                    System.err
                            .println("WARNING: Skipping WARC record due to exception that occured while reading record:");
                    System.err.println(e.getMessage());
                    e.printStackTrace();
                }
            }

            return false;
        }
        
		private String getContentType(WarcRecord arcRecord) {
			String contentTypeHeader = arcRecord.header.contentTypeStr;
			if (arcRecord.getPayload().getPayloadHeaderWrapped() != null && arcRecord.getPayload().getPayloadHeaderWrapped().getHeader(WarcConstants.FN_CONTENT_TYPE) != null)
				contentTypeHeader = arcRecord.getPayload().getPayloadHeaderWrapped().getHeader(WarcConstants.FN_CONTENT_TYPE).value;
			return contentTypeHeader;
		}


        @Override
        public Text createKey()
        {
            return new Text();
        }

        @Override
        public CrawlerRecord createValue()
        {
            return new CrawlerRecord();
        }

        @Override
        public long getPos()
            throws IOException
        {
            return fsin.getCount();
        }

        @Override
        public void close()
            throws IOException
        {
            fsin.close();
        }

        @Override
        public float getProgress()
            throws IOException
        {
            return ((float) (fsin.getCount() - start)) / ((float) (end - start));
        }

        /*
         * ======================== ARC Logic ============================
         */

        /**
         * Skips InputStream to next record past <code>start</code> or not at all if start is
         * exactly the start of a record
         */
        private void skipToNextRecord(long start)
            throws IOException
        {
            // Skip record by record until (position in input stream) >= start
            while (fsin.getCount() < start && warcReader.getNextRecord() != null) {
            }
            ;

            lastRecordEnd = fsin.getCount();
        }
    }
}