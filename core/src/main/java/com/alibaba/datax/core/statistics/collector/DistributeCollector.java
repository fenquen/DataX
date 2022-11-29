package com.alibaba.datax.core.statistics.collector;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;

import java.util.List;

public class DistributeCollector extends AbstractCollector {
    @Override
    public void registerTGCommunication(List<Configuration> taskGroupConfigList) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Communication collectTaskGroup() {
        throw new UnsupportedOperationException();
    }
}
