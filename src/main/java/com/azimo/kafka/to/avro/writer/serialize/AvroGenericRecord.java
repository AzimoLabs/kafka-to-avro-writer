package com.azimo.kafka.to.avro.writer.serialize;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class AvroGenericRecord {
    public int schemaId;
    public GenericRecord record;

    public AvroGenericRecord(int schemaId, GenericRecord record) {
        this.schemaId = schemaId;
        this.record = record;
    }

    public static AvroGenericRecord of(int schemaId, GenericRecord record) {
        return new AvroGenericRecord(schemaId, record);
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }
}
