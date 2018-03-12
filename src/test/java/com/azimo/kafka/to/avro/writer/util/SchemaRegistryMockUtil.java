package com.azimo.kafka.to.avro.writer.util;

import com.azimo.kafka.avro.writer.User;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;

import java.io.IOException;

public class SchemaRegistryMockUtil {
    public static final int SCHEMA_ID = 1;
    public static final int PORT_NUMBER = 8081;

    public static void mockSchema() throws IOException {
        SchemaString schemaString = new SchemaString();
        schemaString.setSchemaString(User.SCHEMA$.toString());
        WireMock.stubFor(WireMock.get(WireMock.urlEqualTo("/schemas/ids/" + SCHEMA_ID))
                .willReturn(WireMock.aResponse()
                        .withHeader("Content-Type","application/json")
                        .withBody(schemaString.toJson())));
    }
}
