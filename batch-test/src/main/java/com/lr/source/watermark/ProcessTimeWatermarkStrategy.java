package com.lr.source.watermark;

import org.apache.flink.api.common.eventtime.*;

/**
 * @author xu.shijie
 * @since 2021/10/13
 */
public class ProcessTimeWatermarkStrategy<T> implements WatermarkStrategy<T> {
    @Override
    public WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new ProcessTimeWatermarkGenerator<>();
    }

    @Override
    public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new ProcessTimeWatermarkTimestampAssigner<>();
    }
}
