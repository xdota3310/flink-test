package com.lr.source.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @author xu.shijie
 * @since 2021/10/13
 */
public class ProcessTimeWatermarkGenerator<T> implements WatermarkGenerator<T> {
    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {

    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // flink水位线默认生成周期是200ms
        output.emitWatermark(new Watermark(System.currentTimeMillis() - 200));
    }
}
