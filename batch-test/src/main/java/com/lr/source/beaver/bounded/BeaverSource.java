package com.lr.source.beaver.bounded;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author xu.shijie
 */
public class BeaverSource extends RichSourceFunction<Tuple2<String, String>> implements CheckpointedFunction {
    public volatile boolean isRunning = true;
    private transient ListState<BloomFilter> bloomFilterValueState;
    private List<BloomFilter<String>> filters = new ArrayList<>();

//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        this.bloomFilterValueState = getRuntimeContext().getOperatorStateStore(
//                new ValueStateDescriptor<>("last_timer", TypeInformation.of(BloomFilter.class)));
//    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        int i = 0;
        BloomFilter<String> filter;
        if (filters.isEmpty()) {
            filter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 1_000_000, 0.04);
            filters.add(filter);
        } else {
            filter = filters.get(0);
        }
        while (this.isRunning && i < 1234) {
            i++;
            System.out.println("bloomfilter: " + filter.put(i + ""));
            ctx.collect(new Tuple2<>(i + "", UUID.randomUUID().toString()));
            Thread.sleep(300);
        }
    }

    @Override
    public void cancel() {
        System.out.println("source cancel");
        this.isRunning = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        bloomFilterValueState.clear();
        for (BloomFilter<String> filter : filters) {
            bloomFilterValueState.add(filter);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<BloomFilter> descriptor =
                new ListStateDescriptor<>("bloomfilter", TypeInformation.of(new TypeHint<BloomFilter>() {}));
        bloomFilterValueState = context.getOperatorStateStore().getListState(descriptor);
        if (context.isRestored()) {
            for (BloomFilter filter : bloomFilterValueState.get()) {
                System.out.println("bloomfilter: " + filter);
                filters.add(filter);
            }
        }
    }
}
