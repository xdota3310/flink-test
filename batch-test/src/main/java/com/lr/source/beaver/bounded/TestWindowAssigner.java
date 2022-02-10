package com.lr.source.beaver.bounded;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author xu.shijie
 * @since 2022/2/10
 */
public class TestWindowAssigner extends WindowAssigner<String, Window> {
    private final Stack<List<Window>> windowS = new Stack<>();

    @Override
    public Collection<Window> assignWindows(String element, long timestamp, WindowAssignerContext context) {
        System.out.println("assignWindows: " + element);
        List<Window> list;
        if (windowS.isEmpty() || windowS.peek().size() == 5) {
            list = new ArrayList<>();
            list.add(new Window() {
                @Override
                public long maxTimestamp() {
                    return 0;
                }
            });
            windowS.push(list);
        } else {
            list = windowS.peek();
        }
        return list;
    }

    @Override
    public Trigger<String, Window> getDefaultTrigger(StreamExecutionEnvironment env) {
        return new Trigger<String, Window>() {
            private AtomicInteger count = new AtomicInteger(0);

            @Override
            public TriggerResult onElement(String element, long timestamp, Window window, TriggerContext ctx) throws Exception {
                if (count.incrementAndGet() % 5 == 0) {
                    return TriggerResult.FIRE_AND_PURGE;
                } else {
                    return TriggerResult.CONTINUE;
                }
            }

            @Override
            public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
                return TriggerResult.CONTINUE;
            }

            @Override
            public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
                return TriggerResult.CONTINUE;
            }

            @Override
            public void clear(Window window, TriggerContext ctx) throws Exception {
            }
        };
    }

    @Override
    public TypeSerializer<Window> getWindowSerializer(ExecutionConfig executionConfig) {
        return new WindowTypeSerializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }
}
