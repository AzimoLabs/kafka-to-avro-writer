package com.azimo.kafka.to.avro.writer.config;

public interface Constants {
    char TOPIC_SEPARATOR = ',';

	String APP_NAME = "Kafka to Avro Writer";
    String FILE_EXTENSION = ".avro";
    String DATE_SUBDIR_FORMAT = "yyyy-MM-dd";

    String AVRO_FILE_NAME_PREFIX = "/events";
}
