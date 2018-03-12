package com.azimo.kafka.to.avro.writer.write;

import com.azimo.kafka.to.avro.writer.config.Constants;
import com.azimo.kafka.to.avro.writer.serialize.AvroDestination;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class WindowedFilenamePolicyTest {
    private static final String INPUT_DIR = "target/output";

    @Test
    public void windowedFilename() {
        //given
        DynamicAvroGenericRecordDestinations destinations = new DynamicAvroGenericRecordDestinations(INPUT_DIR, Constants.FILE_EXTENSION);
        String type = "PaymentMessage";
        AvroDestination destination = AvroDestination.of(type, "");
        FilenamePolicy policy = destinations.getFilenamePolicy(destination);
        DateTime start = new DateTime(2017, 10, 9, 16, 10, DateTimeZone.UTC);
        DateTime end = new DateTime(2017, 10, 9, 17, 10, DateTimeZone.UTC);
        BoundedWindow window = new IntervalWindow(start.toInstant(), end.toInstant());
        PaneInfo paneInfo = PaneInfo.createPane(true, false, Timing.ON_TIME);
        OutputFileHints fileHints = mock(OutputFileHints.class);

        //when
        ResourceId resourceId = policy.windowedFilename(1, 3, window, paneInfo, fileHints);

        //then
        String expectedResource = String.format("target/output/%s/2017-10-09/events-%s-1-of-2-pane-0.avro", type, window);
        assertThat(resourceId.toString()).endsWith(expectedResource);
    }
}