package com.alibaba.datax.core.statistics.communicator;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.DistributeCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.reporter.DistributeReporter;

public class DistributeTGContainerCommunicator extends AbstractTGContainerCommunicator {
    public DistributeTGContainerCommunicator(Configuration configuration) {
        super(configuration);

        abstractReporter = new DistributeReporter();
        abstractCollector = new DistributeCollector();
    }

    @Override
    public void report(Communication communication) {
        abstractReporter.reportTGCommunication(taskGroupId, communication);
    }
}
