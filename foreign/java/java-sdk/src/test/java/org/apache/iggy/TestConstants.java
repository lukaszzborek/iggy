package org.apache.iggy;

import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;

public final class TestConstants {

    private TestConstants() {
    }

    public static final StreamId STREAM_NAME = StreamId.of("test-stream");
    public static final TopicId TOPIC_NAME = TopicId.of("test-topic");

}
