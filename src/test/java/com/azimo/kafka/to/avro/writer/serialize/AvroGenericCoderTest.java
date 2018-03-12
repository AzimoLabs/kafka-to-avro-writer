package com.azimo.kafka.to.avro.writer.serialize;

import com.azimo.kafka.to.avro.writer.util.SchemaRegistryMockUtil;
import com.azimo.kafka.to.avro.writer.generator.UserGenerator;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.testing.CoderProperties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

public class AvroGenericCoderTest {
    @Rule
    public WireMockRule wireMockRule = new WireMockRule(WireMockConfiguration.options().port(SchemaRegistryMockUtil.PORT_NUMBER));
    private String schemaRegistryUrl;
    private AvroGenericCoder coder;

    @Before
    public void before() throws IOException {
        SchemaRegistryMockUtil.mockSchema();
        schemaRegistryUrl = wireMockRule.url("/");
        coder = AvroGenericCoder.of(schemaRegistryUrl);
    }

    @Test
    public void serializeCoder() {
        CoderProperties.coderSerializable(coder);
    }

    @Test
    public void encode() throws Exception {
        GenericRecord record = UserGenerator.createUserGenericRecord();
        AvroGenericRecord avroGenericRecord = AvroGenericRecord.of(SchemaRegistryMockUtil.SCHEMA_ID, record);
         CoderProperties.coderDecodeEncodeEqual(coder, avroGenericRecord);
    }
}