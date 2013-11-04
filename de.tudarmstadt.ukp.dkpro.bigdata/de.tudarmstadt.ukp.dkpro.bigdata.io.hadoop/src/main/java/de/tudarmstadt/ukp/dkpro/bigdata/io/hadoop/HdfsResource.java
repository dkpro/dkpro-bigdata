/*
 * Copyright 2011 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* this class originally was part of spring-data, as soon we have a solution to correctly
 * configure hadoop access from a default constructor with the original class, we should
 * rather include a dependency on spring-data (hpz)
 */
package de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.springframework.core.io.ContextResource;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Resource abstraction over HDFS {@link Path}s.
 * 
 * @author Costin Leau
 */
class HdfsResource
    implements ContextResource
{
    // implements WritableResource,

    private final String location;
    private final Path path;
    private final FileSystem fs;
    private boolean exists;
    private final FileStatus status;

    HdfsResource(String location, FileSystem fs)
    {
        this(location, null, fs);
    }

    HdfsResource(String parent, String child, FileSystem fs)
    {
        this(StringUtils.hasText(child) ? new Path(new Path(URI.create(parent)), new Path(
                URI.create(child))) : new Path(URI.create(parent)), fs);
    }

    @SuppressWarnings("deprecation")
	HdfsResource(Path path, FileSystem fs)
    {
        Assert.notNull(path, "a valid path is required");
        Assert.notNull(fs, "non null file system required");

        this.location = path.toString();
        this.fs = fs;
        this.path = path.makeQualified(fs);

        boolean exists = false;

        try {
            exists = fs.exists(path);
        }
        catch (final Exception ex) {
        }
        this.exists = exists;

        FileStatus status = null;
        try {
            status = fs.getFileStatus(path);
        }
        catch (final Exception ex) {
        }
        this.status = status;
    }

    @Override
    public long contentLength()
        throws IOException
    {
        if (this.exists) {
            if (this.status != null) {
                return this.status.getLen();
            }
        }
        throw new IOException("Cannot access the status for " + getDescription());
    }

    @Override
    public Resource createRelative(String relativePath)
        throws IOException
    {
        return new HdfsResource(this.location, relativePath, this.fs);
    }

    @Override
    public boolean exists()
    {
        return this.exists;
    }

    @Override
    public String getDescription()
    {
        return "HDFS Resource for [" + this.location + "]";
    }

    @Override
    public File getFile()
        throws IOException
    {
        // check for out-of-the-box localFS
        if (this.fs instanceof RawLocalFileSystem) {
            return ((RawLocalFileSystem) this.fs).pathToFile(this.path);
        }

        if (this.fs instanceof LocalFileSystem) {
            return ((LocalFileSystem) this.fs).pathToFile(this.path);
        }

        throw new UnsupportedOperationException("Cannot resolve File object for "
                + getDescription());
    }

    @Override
    public String getFilename()
    {
        return this.path.getName();
    }

    @Override
    public URI getURI()
        throws IOException
    {
        return this.path.toUri();
    }

    @Override
    public URL getURL()
        throws IOException
    {
        return this.path.toUri().toURL();
    }

    @Override
    public boolean isOpen()
    {
        return (this.exists ? true : false);
    }

    @Override
    public boolean isReadable()
    {
        return (this.exists ? true : false);
    }

    @Override
    public long lastModified()
        throws IOException
    {
        if (this.exists && this.status != null) {
            return this.status.getModificationTime();
        }
        throw new IOException("Cannot get timestamp for " + getDescription());
    }

    @Override
    public InputStream getInputStream()
        throws IOException
    {
        if (this.exists) {
            return this.fs.open(this.path);
        }
        throw new IOException("Cannot open stream for " + getDescription());
    }

    /**
     * This implementation returns the description of this resource.
     * 
     * @see #getDescription()
     */
    @Override
    public String toString()
    {
        return getDescription();
    }

    /**
     * This implementation compares description strings.
     * 
     * @see #getDescription()
     */
    @Override
    public boolean equals(Object obj)
    {
        return (obj == this || (obj instanceof Resource && ((Resource) obj).getDescription()
                .equals(getDescription())));
    }

    /**
     * This implementation returns the path hash code.
     */
    @Override
    public int hashCode()
    {
        return this.path.hashCode();
    }

    @Override
    public String getPathWithinContext()
    {
        return this.path.getName();
    }

    public OutputStream getOutputStream()
        throws IOException
    {
        try {
            return this.fs.create(this.path, true);
        }
        finally {
            this.exists = true;
        }
    }

    public boolean isWritable()
    {
        try {
            return ((this.exists && this.fs.isFile(this.path)) || (!this.exists));
        }
        catch (final IOException ex) {
            return false;
        }
    }

    /**
     * Returns the path.
     * 
     * @return Returns the path
     */
    Path getPath()
    {
        return this.path;
    }
}