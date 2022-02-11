package com.lr.source.watermark;

import org.apache.flink.api.common.eventtime.TimestampAssigner;

/**
 * @author xu.shijie
 * @since 2021/10/13
 */
public class ProcessTimeWatermarkTimestampAssigner<T> implements TimestampAssigner<T> {
    @Override
    public long extractTimestamp(T element, long recordTimestamp) {
        return System.currentTimeMillis();
    }
}
