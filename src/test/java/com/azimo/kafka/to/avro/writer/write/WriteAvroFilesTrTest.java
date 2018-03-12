package com.azimo.kafka.to.avro.writer.write;

import com.azimo.kafka.avro.writer.User;
import com.azimo.kafka.to.avro.writer.generator.UserGenerator;
import com.azimo.kafka.to.avro.writer.read.AvroRecord;
import com.azimo.kafka.to.avro.writer.serialize.AvroGenericCoder;
import com.azimo.kafka.to.avro.writer.serialize.AvroGenericRecord;
import com.azimo.kafka.to.avro.writer.util.SchemaRegistryMockUtil;
import com.azimo.kafka.to.avro.writer.read.ReadAvroFilesTr;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.azimo.kafka.to.avro.writer.read.ReadAvroFilesTr.AVRO_FILES_SUFFIX;
import static org.assertj.core.api.Assertions.assertThat;

public class WriteAvroFilesTrTest {
    private static final Path INPUT_DIR = Paths.get("target/input/avrotest/");
    @Rule
    public WireMockRule wireMockRule = new WireMockRule(WireMockConfiguration.options().port(SchemaRegistryMockUtil.PORT_NUMBER));
    private String schemaRegistryUrl;

    @Before
    public void before() throws IOException {
        FileUtils.deleteDirectory(INPUT_DIR.toFile());
        SchemaRegistryMockUtil.mockSchema();
        schemaRegistryUrl = wireMockRule.url("/");
    }

    @Test
    public void testTrade() {
        //given
        Pipeline write = createPipeline();
        List<AvroGenericRecord> expectedRecords = createExpectedRecord();
        GenericRecord expectedGenericRecord = expectedRecords.get(0).record;
        String expectedName = (String) expectedGenericRecord.get("name");
        String expectedUserType = expectedGenericRecord.get("type").toString();

        //when
        write.apply(Create.of(expectedRecords).withCoder(AvroGenericCoder.of(schemaRegistryUrl)))
            .apply(new WriteAvroFilesTr(INPUT_DIR.toAbsolutePath().toString(), 1));
        write.run().waitUntilFinish();

        //then
        Pipeline read = createPipeline();
        List<String> paths = Lists.newArrayList(createReadPath(User.class));
        PCollection<AvroRecord> records = read.apply(new ReadAvroFilesTr(paths));
        PAssert.that(records).satisfies(actual -> {
            User actualRecord = (User) actual.iterator().next().getRecord();
            assertThat(actualRecord.getName()).isEqualTo(expectedName);
            assertThat(actualRecord.getType().name()).isEqualTo(expectedUserType);
            return null;
        });
        read.run().waitUntilFinish();
    }

    private String createReadPath(Class<?> messageClass) {
        StringBuilder sb = new StringBuilder();
        sb.append(INPUT_DIR.toAbsolutePath().toString());
        sb.append("/");
        sb.append(messageClass.getSimpleName());
        sb.append(AVRO_FILES_SUFFIX);
        return sb.toString();
    }

    private List<AvroGenericRecord> createExpectedRecord() {
        List<AvroGenericRecord> records = Lists.newArrayList();
        GenericRecord user = UserGenerator.createUserGenericRecord();
        records.add(AvroGenericRecord.of(SchemaRegistryMockUtil.SCHEMA_ID, user));
        return records;
    }

    private TestPipeline createPipeline() {
        return TestPipeline.create().enableAbandonedNodeEnforcement(false);
    }

}