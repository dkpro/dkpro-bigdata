/*******************************************************************************
 * Copyright 2013
 * Ubiquitous Knowledge Processing (UKP) Lab
 * Technische Universität Darmstadt
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
package org.dkpro.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.resource.ResourceInitializationException;

public interface EngineFactory
{
    /**
     * Implement this method to create the UIMA Analysis Engine Description of the AE that is to be
     * run within the mapper
     * 
     * 
     * @param job
     * @return
     * @throws ResourceInitializationException
     */
    public abstract AnalysisEngineDescription buildMapperEngine(Configuration job)
        throws ResourceInitializationException;

    /**
     * Implement this method to create the UIMA Analysis Engine Description of the AEthat is to be
     * run within the reducer
     * 
     * 
     * @param job
     * @return
     * @throws ResourceInitializationException
     */

    public AnalysisEngineDescription buildReducerEngine(Configuration job)
        throws ResourceInitializationException;

    public void configure(JobConf job);
}
