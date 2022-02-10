package com.lr.source.beaver.bounded;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.io.IOException;

/**
 * @author xu.shijie
 * @since 2022/2/10
 */
public class WindowTypeSerializer extends TypeSerializerSingleton<Window> {
    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public Window createInstance() {
        return null;
    }

    @Override
    public Window copy(Window from) {
        return from;
    }

    @Override
    public Window copy(Window from, Window reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(Window record, DataOutputView target) throws IOException {
        target.writeLong(record.maxTimestamp());
    }

    @Override
    public Window deserialize(DataInputView source) throws IOException {
        return new Window() {
            @Override
            public long maxTimestamp() {
                try {
                    return source.readLong();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return 0L;
            }
        };
    }

    @Override
    public Window deserialize(Window reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeLong(source.readLong());
    }

    @Override
    public TypeSerializerSnapshot<Window> snapshotConfiguration() {
        return new SimpleTypeSerializerSnapshot<Window>(WindowTypeSerializer::new){};
    }
}
