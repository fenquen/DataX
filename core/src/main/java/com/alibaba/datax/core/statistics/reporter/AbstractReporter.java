package com.alibaba.datax.core.statistics.reporter;

import com.alibaba.datax.core.statistics.communication.Communication;

public abstract class AbstractReporter {

    public abstract void reportJobCommunication(Long jobId, Communication communication);

    /**
     * 把 taskGroup维度的 communication 上报落地
     */
    public abstract void reportTGCommunication(Integer taskGroupId, Communication communication);

}
