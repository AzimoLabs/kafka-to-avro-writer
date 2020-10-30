package com.azimo.kafka.to.avro.writer.write;

import com.azimo.kafka.to.avro.writer.serialize.AvroDestination;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class DynamicAvroFileNamingTest {
    private static final String INPUT_DIR = "target/output";
    private static final String TEMP_DIR = INPUT_DIR +"/temp";

    @Test
    public void windowedFilename() {
        //given
        String type = "PaymentMessage";
        AvroDestination destination = AvroDestination.of(type, "{}");
        WriteAvroFilesTr writeTr = new WriteAvroFilesTr(INPUT_DIR, TEMP_DIR, 1);
        WriteAvroFilesTr.DynamicAvroFileNaming policy = writeTr.new DynamicAvroFileNaming(destination);
        DateTime start = new DateTime(2017, 10, 9, 16, 10, DateTimeZone.UTC);
        DateTime end = new DateTime(2017, 10, 9, 17, 10, DateTimeZone.UTC);
        BoundedWindow window = new IntervalWindow(start.toInstant(), end.toInstant());
        PaneInfo paneInfo = PaneInfo.createPane(true, false, Timing.ON_TIME);
        OutputFileHints fileHints = mock(OutputFileHints.class);

        //when
        String filename = policy.getFilename(window, paneInfo, 3, 1, Compression.AUTO);

        //then
        String expectedResourcePrefix = String.format("%s/dt=2017-10-09/events-%s-", type, DigestUtils.md5Hex(destination.jsonSchema));
        String expectedResourceSuffix = String.format("-%s-00001-of-00003-pane-0.avro", window);
        assertThat(filename).contains(expectedResourcePrefix);
        assertThat(filename).contains(expectedResourceSuffix);
    }
}