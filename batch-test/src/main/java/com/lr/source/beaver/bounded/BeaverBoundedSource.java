package com.lr.source.beaver.bounded;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * @author xu.shijie
 * @since 2022/2/10
 */
public class BeaverBoundedSource implements Source {
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader createReader(SourceReaderContext readerContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator createEnumerator(SplitEnumeratorContext enumContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator restoreEnumerator(SplitEnumeratorContext enumContext, Object checkpoint) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer getEnumeratorCheckpointSerializer() {
        return null;
    }
}
