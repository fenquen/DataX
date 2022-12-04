package com.alibaba.datax.core.statistics.communicator;


import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.collector.Collector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.reporter.Reporter;
import com.alibaba.datax.core.util.container.CoreConstant;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * collector reporter
 */
@Data
public abstract class AbstractContainerCommunicator {
    protected Configuration configuration;

    protected Collector collector = new Collector();

    protected Reporter reporter = new Reporter();

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

    public abstract void addCommunication(List<Configuration> configurationList);

    public abstract Communication collect();

    public abstract void report(Communication communication);

    public abstract Communication getCommunication(Integer id);

    /**
     * 得到了各自的 taskGroupId_communication taskId_communication 账单信息
     */
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