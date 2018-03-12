package com.azimo.kafka.to.avro.writer.config;

import com.google.common.base.Splitter;

import java.util.List;

public class ConfigUtils {
	
	public static List<String> convertCsvToList(String inputTopics, char topicSeparator) {
		return Splitter.on(topicSeparator)
				.trimResults()
				.omitEmptyStrings()
				.splitToList(inputTopics);
	}

}
