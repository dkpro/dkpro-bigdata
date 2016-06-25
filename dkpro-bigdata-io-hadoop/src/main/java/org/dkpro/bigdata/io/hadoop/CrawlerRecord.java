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

import java.util.Date;

/**
 * Stores (raw!) data and meta data of a crawler record that's relevant to us
 * 
 * @author Johannes Simon
 *
 */
public class CrawlerRecord {
	
	public CrawlerRecord() {
	}
	
	/**
	 * URL of record. Must not be null.
	 */
	private String url;
	public void setURL(String url) { this.url = url; }
	public String getURL() { return url; }

	/**
	 * Original text content (potentially with HTML markup etc.) of record. Must not be null.
	 */
	private String content;
	public void setContent(String content) { this.content = content; }
	public String getContent() { return content; }

	/**
	 * Specifies whether <code>content</code> contains any markup, e.g. HTML, or is plain text.
	 */
	private boolean isHTML;
	public void setIsHTML(boolean isHTML) { this.isHTML = isHTML; }
	public boolean isHTML() { return isHTML; }

	/**
	 * Original encoding of record. May be null.
	 */
	private String origEncoding;
	public void setOriginalEncoding(String origEncoding) { this.origEncoding = origEncoding; }
	public String getOriginalEncoding() { return origEncoding; }

	/**
	 * Original language of record. May be null.
	 */
	private String origLanguage;
	public void setOriginalLanguage(String origLanguage) { this.origLanguage = origLanguage; }
	public String getOriginalLanguage() { return origLanguage; }

	/**
	 * Name of machine that crawled this record. May be null.
	 */
	private String user;
	public void setUser(String user) { this.user = user; }
	public String getUser() { return user; }

	/**
	 * Date this record was crawled. May be null.
	 */
	private Date date;
	public void setDate(Date date) { this.date = date; }
	public Date getDate() { return date; }
}
