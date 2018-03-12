package com.azimo.kafka.to.avro.writer.serialize;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.util.EmptyOnDeserializationThreadLocal;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class BeamKafkaAvroGenericSerializer {
    private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();
    private final EmptyOnDeserializationThreadLocal<BinaryEncoder> encoder;

    public BeamKafkaAvroGenericSerializer() {
        encoder = new EmptyOnDeserializationThreadLocal<>();
    }

    protected void serialize(AvroGenericRecord avroGenericRecord, OutputStream out) throws SerializationException {
        if (avroGenericRecord != null) {
            try {
                GenericRecord value = avroGenericRecord.record;
                Schema schema = value.getSchema();
                out.write(0);
                out.write(ByteBuffer.allocate(4).putInt(avroGenericRecord.schemaId).array());

                BinaryEncoder encoderInstance = ENCODER_FACTORY.directBinaryEncoder(out, encoder.get());
                encoder.set(encoderInstance);
                GenericDatumWriter writer = new GenericDatumWriter(schema);
                writer.write(value, encoderInstance);
            } catch (RuntimeException | IOException var9) {
                throw new SerializationException("Error serializing Avro message", var9);
            }
        }
    }
}
