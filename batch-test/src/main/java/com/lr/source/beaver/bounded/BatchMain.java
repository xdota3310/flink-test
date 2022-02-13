package com.lr.source.beaver.bounded;

import com.lr.source.watermark.ProcessTimeWatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ReduceApplyProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class BatchMain implements Serializable {
    public static void main(String[] args) throws Exception {
        BatchMain main = new BatchMain();
        main.run();
    }

    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(4);
        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);
        env.addSource(new BeaverSource()).assignTimestampsAndWatermarks(new ProcessTimeWatermarkStrategy<>()).flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
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
            private final AtomicInteger count = new AtomicInteger(0);

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
        }).windowAll(TumblingEventTimeWindows.of(Time.minutes(100))).process(new ReduceApplyProcessAllWindowFunction<>(new ReduceFunction<List<String>>() {
            @Override
            public List<String> reduce(List<String> value1, List<String> value2) throws Exception {
                value1.addAll(value2);
                return value1;
            }
        }, new ProcessAllWindowFunction<List<String>, String, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<List<String>, String, TimeWindow>.Context context, Iterable<List<String>> elements, Collector<String> out) throws Exception {
                for (List<String> element : elements) {
                    String result = String.join("~~~~~~~~~~~~~~", element);
                    System.out.println("ProcessAllWindowFunction");
                    out.collect(result);
                }
            }
        }), TypeInformation.of(String.class)).addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                SinkFunction.super.invoke(value, context);
                System.out.println("sink: " + value);
            }
        });
        env.execute(UUID.randomUUID().toString());
    }
}
