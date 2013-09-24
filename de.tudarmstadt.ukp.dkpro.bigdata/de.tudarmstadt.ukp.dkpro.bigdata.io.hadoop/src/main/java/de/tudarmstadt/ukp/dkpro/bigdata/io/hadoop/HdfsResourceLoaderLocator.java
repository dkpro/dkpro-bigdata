package de.tudarmstadt.ukp.dkpro.bigdata.io.hadoop;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.uima.fit.component.Resource_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.descriptor.ExternalResourceLocator;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

import de.tudarmstadt.ukp.dkpro.core.api.io.ResourceLoaderLocator;

public class HdfsResourceLoaderLocator
    extends Resource_ImplBase
    implements ExternalResourceLocator
{
    private static final String PARAM_FILESYSTEM = "fileSystem";
    @ConfigurationParameter(name = PARAM_FILESYSTEM, mandatory = false)
    private String fileSystem;

    private ResourcePatternResolver resolverInstance;

    @Override
    public boolean initialize(ResourceSpecifier aSpecifier, Map<String, Object> aAdditionalParams)
        throws ResourceInitializationException
    {
        try {
            if (fileSystem == null)
                new HdfsResourceLoader(new Configuration(true));
            else
                resolverInstance = new HdfsResourceLoader(new Configuration(), new URI(fileSystem));
        }
        catch (URISyntaxException e) {
            throw new ResourceInitializationException(e);
        }
        return true;

    }

    @Override
    public Object getResource()
    {
        return resolverInstance;
    }

}
