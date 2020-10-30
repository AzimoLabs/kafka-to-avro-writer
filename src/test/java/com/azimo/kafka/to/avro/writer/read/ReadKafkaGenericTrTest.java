package com.azimo.kafka.to.avro.writer.read;

import com.azimo.kafka.avro.writer.User;
import com.azimo.kafka.to.avro.writer.generator.UserGenerator;
import com.azimo.kafka.to.avro.writer.serialize.AvroGenericRecord;
import com.azimo.kafka.to.avro.writer.util.SchemaRegistryMockUtil;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.Lists;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.List;
import java.util.Map;

public class ReadKafkaGenericTrTest {
	private static final String TOPIC_USER = "user";

	@Rule
	public WireMockRule wireMockRule = new WireMockRule(WireMockConfiguration.options().port(SchemaRegistryMockUtil.PORT_NUMBER));
	private String schemaRegistryUrl;

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, false, 1, TOPIC_USER);

	private KafkaProducer<String, Object> producer;

	@Before
	public void before() throws Exception {
		Map<String, Object> props = KafkaTestUtils.producerProps(getBrokers());
		MockSchemaRegistryClient client = new MockSchemaRegistryClient();
		client.register(TOPIC_USER + "-value", User.SCHEMA$);
		KafkaAvroSerializer valueSerializer = new KafkaAvroSerializer(client);
		StringSerializer keySerializer = new StringSerializer();
		producer = new KafkaProducer<>(props, keySerializer, valueSerializer);
		SchemaRegistryMockUtil.mockSchema();
		schemaRegistryUrl = wireMockRule.url("/");
	}

	private String getBrokers() {
		return embeddedKafkaRule.getEmbeddedKafka().getBrokersAsString();
	}

	@Test
	public void test() {
		//given
		Pipeline p = createPipeline();


		GenericRecord expectedRecord = UserGenerator.createUserGenericRecord();
		List<AvroGenericRecord> expectedOutput = Lists.newArrayList(AvroGenericRecord.of(SchemaRegistryMockUtil.SCHEMA_ID, expectedRecord));

		producer.send(new ProducerRecord<>(TOPIC_USER, UserGenerator.createUserSpecificRecord()));

		//when
		PCollection<AvroGenericRecord> output = p
				.apply(createReadTr(expectedOutput));



		//then
		PAssert.that(output).containsInAnyOrder(expectedOutput);
		p.run().waitUntilFinish();
	}

	private ReadKafkaGenericTr createReadTr(List<AvroGenericRecord> expectedOutput) {
		return ReadKafkaGenericTr.newReadKafkaGenericTrBuilder()
				.inputTopics(Lists.newArrayList(TOPIC_USER))
				.bootstrapServers(getBrokers())
				.schemaRegistryUrl(schemaRegistryUrl)
				.offsetReset("earliest")
				.consumerGroupId("testgroup")
				.maxNumRecords(expectedOutput.size())
				.build();
	}

	private TestPipeline createPipeline() {
		return TestPipeline.create().enableAbandonedNodeEnforcement(false);
	}
}
