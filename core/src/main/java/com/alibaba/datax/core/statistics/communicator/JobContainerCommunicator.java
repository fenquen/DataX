package com.alibaba.datax.core.statistics.communicator;


import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * 管理的是taskGroup
 */
public class JobContainerCommunicator extends AbstractContainerCommunicator {
    private static final Logger LOG = LoggerFactory.getLogger(JobContainerCommunicator.class);

    public JobContainerCommunicator(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void addCommunication(List<Configuration> taskGroupConfigList) {
        collector.addTGCommunication(taskGroupConfigList);
    }

    @Override
    public Communication collect() {
        return collector.collectTaskGroup();
    }

    /**
     * 和 DistributeJobContainerCollector 的 report 实现一样
     */
    @Override
    public void report(Communication communication) {
        reporter.reportJobCommunication(jobId, communication);

        LOG.info(CommunicationTool.Stringify.getSnapshot(communication));
        reportVmInfo();
    }

    @Override
    public Communication getCommunication(Integer taskGroupId) {
        return collector.getTGCommunication(taskGroupId);
    }

    @Override
    public Map<Integer, Communication> getCommunicationMap() {
        return LocalTGCommunicationManager.taskGroupId_communication;
    }
}
