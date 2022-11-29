package com.alibaba.datax.core.statistics.communicator;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.LocalCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.statistics.reporter.LocalReporter;
import com.alibaba.datax.core.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class LocalJobContainerCommunicator extends AbstractContainerCommunicator {
    private static final Logger LOG = LoggerFactory.getLogger(LocalJobContainerCommunicator.class);

    public LocalJobContainerCommunicator(Configuration configuration) {
        super(configuration);

        abstractCollector = new LocalCollector();
        abstractReporter = new LocalReporter();
    }

    @Override
    public void registerCommunication(List<Configuration> taskGroupConfigList) {
        abstractCollector.registerTGCommunication(taskGroupConfigList);
    }

    @Override
    public Communication collect() {
        return abstractCollector.collectTaskGroup();
    }

    @Override
    public State collectState() {
        return collect().getState();
    }

    /**
     * 和 DistributeJobContainerCollector 的 report 实现一样
     */
    @Override
    public void report(Communication communication) {
        abstractReporter.reportJobCommunication(jobId, communication);

        LOG.info(CommunicationTool.Stringify.getSnapshot(communication));
        reportVmInfo();
    }

    @Override
    public Communication getCommunication(Integer taskGroupId) {
        return abstractCollector.getTGCommunication(taskGroupId);
    }

    @Override
    public Map<Integer, Communication> getCommunicationMap() {
        return LocalTGCommunicationManager.taskGroupId_communication;
    }
}
