package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;

public class ProcessInnerCollector extends AbstractCollector {
    public ProcessInnerCollector(Long jobId) {
        this.jobId = jobId;
    }

    @Override
    public Communication collectTaskGroup() {
        return LocalTGCommunicationManager.getJobCommunication();
    }
}
