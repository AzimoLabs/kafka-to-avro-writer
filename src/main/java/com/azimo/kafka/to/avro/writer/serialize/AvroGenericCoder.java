package com.azimo.kafka.to.avro.writer.serialize;

import com.google.common.collect.Maps;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public class AvroGenericCoder extends CustomCoder<AvroGenericRecord> {
    private final String registryUrl;
    private transient BeamKafkaAvroGenericDeserializer deserializer;
    private transient BeamKafkaAvroGenericSerializer serializer;

    public static AvroGenericCoder of(String registryUrl) {
        return new AvroGenericCoder(registryUrl);
    }

    protected AvroGenericCoder(String registryUrl) {
        this.registryUrl = registryUrl;
    }

    private BeamKafkaAvroGenericDeserializer getDeserializer() {
        if (deserializer == null) {
            BeamKafkaAvroGenericDeserializer d = new BeamKafkaAvroGenericDeserializer();
            d.configure(createRegistryUrlMap(), false);
            deserializer = d;
        }
        return deserializer;
    }

    private Map<String, String> createRegistryUrlMap() {
        Map<String, String> map = Maps.newHashMap();
        map.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        return map;
    }

    private BeamKafkaAvroGenericSerializer getSerializer() {
        if (serializer == null) {
            serializer = new BeamKafkaAvroGenericSerializer();
        }
        return serializer;
    }

    @Override
    public void encode(AvroGenericRecord record, OutputStream outStream) {
        getSerializer().serialize(record, outStream);
    }

    @Override
    public AvroGenericRecord decode(InputStream inStream) {
        return getDeserializer().deserialize(inStream);
    }

    @Override
    public void verifyDeterministic() {
        //We operate only on generic records using schema registry so we assume everything is deterministic
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
