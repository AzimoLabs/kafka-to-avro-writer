package com.azimo.kafka.to.avro.writer.config;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface KafkaToAvroWriterPipelineOptions extends DataflowPipelineOptions {
	@Description("Base write path (on prod path should point to Gcs bucket")
	@Validation.Required
	String getBasePath();
	void setBasePath(String value);

	@Description("Number of shards per event type")
	@Default.Integer(3)
	int getNumberOfShards();
	void setNumberOfShards(int value);

	@Description("Window size in minutes")
	@Default.Integer(60)
	int getWindowInMinutes();
	void setWindowInMinutes(int value);


	@Description("Kafka input topics in CSV format")
	@Validation.Required
	String getInputTopics();
	void setInputTopics(String value);

	@Description("Kafka bootstrap servers")
	@Validation.Required
	String getBootstrapServers();
	void setBootstrapServers(String value);

	@Description("Schema registry url")
	@Validation.Required
	String getSchemaRegistryUrl();
	void setSchemaRegistryUrl(String value);

	@Description("Kafka consumer auto offset reset: [latest, earliest, none]")
	@Default.String("latest")
	String getOffsetReset();
	void setOffsetReset(String value);

	@Description("Kafka - a unique string that identifies the consumer group this consumer belongs to")
	String getConsumerGroupId();
	void setConsumerGroupId(String value);

	@Description("Kafka - if set to true offsets are committed automatically with a frequency controlled by the config auto.commit.interval.ms (default 5000).")
	@Default.Boolean(true)
	boolean isEnableAutoCommit();
	void setEnableAutoCommit(boolean value);

}
