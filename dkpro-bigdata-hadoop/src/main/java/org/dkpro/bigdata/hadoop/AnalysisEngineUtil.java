/*******************************************************************************
 * Copyright 2013 Ubiquitous Knowledge Processing (UKP) Lab Technische Universität Darmstadt
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 ******************************************************************************/
package org.dkpro.bigdata.hadoop;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.metadata.AnalysisEngineMetaData;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.ConfigurationParameter;
import org.apache.uima.resource.metadata.ConfigurationParameterDeclarations;
import org.apache.uima.resource.metadata.ConfigurationParameterSettings;
import org.apache.uima.util.InvalidXMLException;

public class AnalysisEngineUtil {


	/**
	 * Search for primitive engineDescriptions and replace some variables in
	 * parameters.
	 * 
	 * Search also in nested engineDescriptions
	 * 
	 * @param engineDescription
	 * @throws InvalidXMLException
	 */
	public static void replaceVariables(AnalysisEngineDescription engineDescription, Map<String, String> replacements)
			throws InvalidXMLException {
		if (replacements.isEmpty()) {
			return;
		}
		AnalysisEngineMetaData analysisEngineMetaData = engineDescription
				.getAnalysisEngineMetaData();
		ConfigurationParameterDeclarations configurationParameterDeclarations = analysisEngineMetaData
				.getConfigurationParameterDeclarations();

			replaceVariables(analysisEngineMetaData,
					configurationParameterDeclarations,
					replacements);

		for (final Entry<String, ResourceSpecifier> e : engineDescription
				.getDelegateAnalysisEngineSpecifiers().entrySet()) {
			ResourceSpecifier rs = e.getValue();
			if (rs instanceof AnalysisEngineDescription) {
				replaceVariables((AnalysisEngineDescription) rs, replacements);
			}
		}
	}

	/**
	 * Replace variables in the UIMA-Engines configuration to use the resources
	 * provides by hadoop.
	 * 
	 * @param analysisEngineMetaData
	 * @param configurationParameterDeclarations
	 */
	private static void replaceVariables(
			AnalysisEngineMetaData analysisEngineMetaData,
			ConfigurationParameterDeclarations configurationParameterDeclarations,
			Map<String, String> replacements) {
		for (final ConfigurationParameter parameter : configurationParameterDeclarations
				.getConfigurationParameters()) {
			final ConfigurationParameterSettings configurationParameterSettings = analysisEngineMetaData
					.getConfigurationParameterSettings();
			final Object parameterValue = configurationParameterSettings
					.getParameterValue(parameter.getName());
			if (parameterValue instanceof String) {
				String paramValue = (String)parameterValue;
				for(Entry<String, String> replacement : replacements.entrySet()) {
					paramValue = paramValue.replaceAll(replacement.getKey(), replacement.getValue());
				}
				configurationParameterSettings.setParameterValue(parameter.getName(), paramValue);
			}
		}
	}
}
