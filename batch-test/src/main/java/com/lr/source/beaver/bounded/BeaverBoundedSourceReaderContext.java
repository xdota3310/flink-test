package com.lr.source.beaver.bounded;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;

/**
 * @author xu.shijie
 * @since 2022/2/10
 */
public class BeaverBoundedSourceReaderContext implements SourceReaderContext {
    @Override
    public MetricGroup metricGroup() {
        return null;
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }

    @Override
    public String getLocalHostName() {
        return null;
    }

    @Override
    public int getIndexOfSubtask() {
        return 0;
    }

    @Override
    public void sendSplitRequest() {

    }

    @Override
    public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {

    }
}
