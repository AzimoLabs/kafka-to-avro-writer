package com.azimo.kafka.to.avro.writer.read;


import com.azimo.kafka.to.avro.writer.serialize.AvroGenericCoder;
import com.azimo.kafka.to.avro.writer.serialize.AvroGenericRecord;
import com.azimo.kafka.to.avro.writer.serialize.BeamKafkaAvroGenericDeserializer;
import com.google.common.collect.Maps;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Map;

@SuppressWarnings("serial")
public class ReadKafkaGenericTr extends PTransform<PBegin, PCollection<AvroGenericRecord>> {
	private List<String> inputTopics;
	private String bootstrapServers;
	private String schemaRegistryUrl;
	private String offsetReset;
	private String consumerGroupId;
	private long maxNumRecords;
	private boolean isEnableAutoCommit;

	private ReadKafkaGenericTr(Builder builder) {
		this.inputTopics = builder.inputTopics;
		this.bootstrapServers = builder.bootstrapServers;
		this.schemaRegistryUrl = builder.schemaRegistryUrl;
		this.offsetReset = builder.offsetReset;
		this.consumerGroupId = builder.consumerGroupId;
		this.maxNumRecords = builder.maxNumRecords;
		this.isEnableAutoCommit = builder.isEnableAutoCommit;
	}

    public static Builder newReadKafkaGenericTrBuilder() {
        return new Builder();
    }

    @Override
	public PCollection<AvroGenericRecord> expand(PBegin p) {
		Map<String, Object> configUpdates = configure();
		return p.apply("Read from kafka", createKafkaRead(configUpdates))
				.apply("Map AvroGenericRecord", MapElements.into(TypeDescriptor.of(AvroGenericRecord.class))
						.via(KV::getValue)).setCoder(AvroGenericCoder.of(schemaRegistryUrl));

	}

	private PTransform<PBegin, PCollection<KV<String, AvroGenericRecord>>> createKafkaRead(Map<String, Object> configUpdates) {
		return KafkaIO.<String, AvroGenericRecord>read()
				.withBootstrapServers(bootstrapServers)
				.updateConsumerProperties(configUpdates)
				.withTopics(inputTopics)
				.withKeyDeserializer(StringDeserializer.class)
				.withValueDeserializerAndCoder(BeamKafkaAvroGenericDeserializer.class, AvroGenericCoder.of(schemaRegistryUrl))
				.withMaxNumRecords(maxNumRecords)
				.withoutMetadata();
	}

	private Map<String, Object> configure() {
		Map<String, Object> configUpdates = Maps.newHashMap();
		configUpdates.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
		configUpdates.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
		configUpdates.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
		configUpdates.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, isEnableAutoCommit);
		return configUpdates;
	}

    public static final class Builder {
		private List<String> inputTopics;
		private String bootstrapServers;
		private String schemaRegistryUrl;
		private String offsetReset;
		private String consumerGroupId;
		private long maxNumRecords = Long.MAX_VALUE;
        private boolean isEnableAutoCommit;

        private Builder() {
		}

		public ReadKafkaGenericTr build() {
			return new ReadKafkaGenericTr(this);
		}

		public Builder inputTopics(List<String> inputTopics) {
			this.inputTopics = inputTopics;
			return this;
		}

		public Builder bootstrapServers(String bootstrapServers) {
			this.bootstrapServers = bootstrapServers;
			return this;
		}

		public Builder schemaRegistryUrl(String schemaRegistryUrl) {
			this.schemaRegistryUrl = schemaRegistryUrl;
			return this;
		}

		public Builder offsetReset(String offsetReset) {
			this.offsetReset = offsetReset;
			return this;
		}

		public Builder consumerGroupId(String consumerGroupId) {
			this.consumerGroupId = consumerGroupId;
			return this;
		}

		public Builder maxNumRecords(long maxNumRecords) {
			this.maxNumRecords = maxNumRecords;
			return this;
		}

		public Builder isEnableAutoCommit(boolean isEnableAutoCommit) {
			this.isEnableAutoCommit = isEnableAutoCommit;
			return this;
		}
	}
}
