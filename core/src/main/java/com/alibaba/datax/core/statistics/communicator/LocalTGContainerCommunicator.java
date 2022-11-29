package com.alibaba.datax.core.statistics.communicator;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.LocalCollector;
import com.alibaba.datax.core.statistics.reporter.LocalReporter;
import com.alibaba.datax.core.statistics.communication.Communication;

public class LocalTGContainerCommunicator extends AbstractTGContainerCommunicator {

    public LocalTGContainerCommunicator(Configuration configuration) {
        super(configuration);

        abstractReporter = new LocalReporter();
        abstractCollector = new LocalCollector();
    }

    @Override
    public void report(Communication communication) {
        abstractReporter.reportTGCommunication(taskGroupId, communication);
    }
}
