package com.alibaba.datax.core.statistics.reporter;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;

public class LocalReporter extends AbstractReporter {

    @Override
    public void reportJobCommunication(Long jobId, Communication communication) {

    }

    @Override
    public void reportTGCommunication(Integer taskGroupId, Communication communication) {
        LocalTGCommunicationManager.update(taskGroupId, communication);
    }
}