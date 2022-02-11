package com.lr.source.beaver.bounded;

import com.lr.source.watermark.ProcessTimeWatermarkStrategy;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchMain implements Serializable {
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        env.addSource(new SourceFunction<Tuple2<String, String>>() {
            public volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
                int i = 0;
                while (this.isRunning && i < 11) {
                    i++;
                    ctx.collect(new Tuple2<>(i + "", UUID.randomUUID().toString()));
                    Thread.sleep(500);
                }
            }

            @Override
            public void cancel() {
                this.isRunning = false;
            }
        }).assignTimestampsAndWatermarks(new ProcessTimeWatermarkStrategy<>()).flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public void flatMap(Tuple2<String, String> value, Collector<String> out) throws Exception {
                System.out.println("flatMap: " + value.f0);
                out.collect(value.f0 + ": " + value.f1);
            }
        }).rebalance().keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return "";
            }
        }).window(TumblingEventTimeWindows.of(Time.minutes(1))).trigger(new Trigger<String, TimeWindow>() {
            private AtomicInteger count = new AtomicInteger(0);
            @Override
            public TriggerResult onElement(String element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                if (count.incrementAndGet() % 5 == 0) {
                    count.set(0);
                    return TriggerResult.FIRE_AND_PURGE;
                } else {
                    return TriggerResult.CONTINUE;
                }
            }

            @Override
            public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                return TriggerResult.CONTINUE;
            }

            @Override
            public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                System.out.println("onEventTime: " + time);
                return TriggerResult.FIRE_AND_PURGE;
            }

            @Override
            public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

            }
        }).process(new ProcessWindowFunction<String, List<String>, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<String, List<String>, String, TimeWindow>.Context context, Iterable<String> elements, Collector<List<String>> out) throws Exception {
                List<String> result = new ArrayList<>();
                for (String element : elements) {
                    System.out.println("process: " + element);
                    result.add(element);
                }
                out.collect(result);
            }
        }).addSink(new SinkFunction<List<String>>() {
            @Override
            public void invoke(List<String> value, Context context) throws Exception {
                SinkFunction.super.invoke(value, context);
                System.out.println("sink: " + value);
            }
        });
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        BatchMain main = new BatchMain();
        main.run();
    }
}
