package com.alibaba.datax.core.statistics.container.communicator;


import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.container.collector.AbstractCollector;
import com.alibaba.datax.core.statistics.container.report.AbstractReporter;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public abstract class AbstractContainerCommunicator {
    protected Configuration configuration;

    protected AbstractCollector abstractCollector;

    protected AbstractReporter abstractReporter;

    protected Long jobId;

    private VMInfo vmInfo = VMInfo.getVmInfo();

    private long lastReportTime = System.currentTimeMillis();

    public Long getJobId() {
        return jobId;
    }

    public AbstractContainerCommunicator(Configuration configuration) {
        this.configuration = configuration;
        jobId = configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
    }

    public abstract void registerCommunication(List<Configuration> configurationList);

    public abstract Communication collect();

    public abstract void report(Communication communication);

    public abstract State collectState();

    public abstract Communication getCommunication(Integer id);

    public abstract Map<Integer, Communication> getCommunicationMap();

    public void resetCommunication(Integer id) {
       getCommunicationMap().put(id, new Communication());
    }

    public void reportVmInfo() {
        long now = System.currentTimeMillis();

        // 每5分钟打印一次
        if (now - lastReportTime >= 300000) {
            if (vmInfo != null) {
                vmInfo.getDelta(true);
            }

            lastReportTime = now;
        }
    }
}