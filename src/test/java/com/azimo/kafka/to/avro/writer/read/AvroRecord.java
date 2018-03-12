package com.azimo.kafka.to.avro.writer.read;

import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import java.io.Serializable;

public class AvroRecord implements Serializable {
	private final IndexedRecord record;

	private AvroRecord(IndexedRecord record) {
		this.record = record;
	}
	
	public static AvroRecord of(IndexedRecord record) {
		return new AvroRecord(record);
	}

	public IndexedRecord getRecord() {
		return record;
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
