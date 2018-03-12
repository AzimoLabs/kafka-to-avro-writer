package com.azimo.kafka.to.avro.writer.serialize;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.util.EmptyOnDeserializationThreadLocal;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class BeamKafkaAvroGenericDeserializer implements Deserializer<AvroGenericRecord> {
    private final static DecoderFactory DECODER_FACTORY = DecoderFactory.get();
    private final EmptyOnDeserializationThreadLocal<BinaryDecoder> decoder;
    private SchemaRegistryClient schemaRegistry;

    public BeamKafkaAvroGenericDeserializer() {
        decoder = new EmptyOnDeserializationThreadLocal<>();
    }

    @Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		configureClientProperties(new KafkaAvroDeserializerConfig(configs));
	}

    protected void configureClientProperties(AbstractKafkaAvroSerDeConfig config) {
        try {
            List<String> urls = config.getSchemaRegistryUrls();
            int maxSchemaObject = config.getMaxSchemasPerSubject();

            if (null == schemaRegistry) {
                schemaRegistry = new CachedSchemaRegistryClient(urls, maxSchemaObject);
            }
        } catch (io.confluent.common.config.ConfigException e) {
            throw new ConfigException(e.getMessage());
        }
    }

	@Override
	public AvroGenericRecord deserialize(String topic, byte[] payload) {
		if (payload == null) {
			return null;
		}
		return deserialize(new ByteArrayInputStream(payload));
	}

    public AvroGenericRecord deserialize(InputStream in) {
        int id = -1;
        try {
            if (in.read() != 0) {
                throw new SerializationException("Unknown magic byte!");
            }
            byte[] idArray = ByteBuffer.allocate(4).array();
            in.read(idArray, 0, 4);
            id = ByteBuffer.wrap(idArray).getInt();

            BinaryDecoder decoderInstance = DECODER_FACTORY.directBinaryDecoder(in, decoder.get());
            decoder.set(decoderInstance);

            Schema schema = this.schemaRegistry.getByID(id);
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            GenericRecord object = reader.read(null, decoderInstance);
            return AvroGenericRecord.of(id, object);
        } catch (RuntimeException | IOException var15) {
            throw new SerializationException("Error deserializing Avro message for id " + id, var15);
        } catch (RestClientException var16) {
            throw new SerializationException("Error retrieving Avro schema for id " + id, var16);
        }
    }

	@Override
	public void close() {}
}
