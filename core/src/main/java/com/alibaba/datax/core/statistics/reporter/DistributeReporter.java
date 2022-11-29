package com.alibaba.datax.core.statistics.reporter;

import com.alibaba.datax.core.statistics.communication.Communication;

public class DistributeReporter extends AbstractReporter {
    @Override
    public void reportJobCommunication(Long jobId, Communication communication) {

    }

    @Override
    public void reportTGCommunication(Integer taskGroupId, Communication communication) {
        throw new UnsupportedOperationException();
    }
}
