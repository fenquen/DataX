package com.alibaba.datax.core.job.scheduler;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.util.ErrorRecordChecker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractScheduler.class);

    private ErrorRecordChecker errorLimit;

    private AbstractContainerCommunicator abstractContainerCommunicator;

    private Long jobId;

    public Long getJobId() {
        return jobId;
    }

    public AbstractScheduler(AbstractContainerCommunicator abstractContainerCommunicator) {
        this.abstractContainerCommunicator = abstractContainerCommunicator;
    }

    public void schedule(List<Configuration> taskGroupConfigList) {
        int jobReportIntervalMs = taskGroupConfigList.get(0).getInt(CoreConstant.DATAX_CORE_CONTAINER_JOB_REPORTINTERVAL, 30000);
        int jobSleepIntervalMs = taskGroupConfigList.get(0).getInt(CoreConstant.DATAX_CORE_CONTAINER_JOB_SLEEPINTERVAL, 10000);

        jobId = taskGroupConfigList.get(0).getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);

        errorLimit = new ErrorRecordChecker(taskGroupConfigList.get(0));

        // 给 taskGroupContainer 的 Communication 注册
        abstractContainerCommunicator.registerCommunication(taskGroupConfigList);

        int totalTasks = calculateTaskCount(taskGroupConfigList);

        // 是不是分布式这里有体现
        startAllTaskGroup(taskGroupConfigList);

        Communication mergedAllTgCommLast = new Communication();

        long lastReportTimeStamp = System.currentTimeMillis();
        try {
            while (true) {
                Communication mergedAllTgComm = abstractContainerCommunicator.collect(); // collect的是 task group的
                mergedAllTgComm.setTimestamp(System.currentTimeMillis());

                // 汇报周期
                long now = System.currentTimeMillis();

                if (now - lastReportTimeStamp > jobReportIntervalMs) {
                    Communication reportCommunication =
                            CommunicationTool.getReportComm(mergedAllTgComm, mergedAllTgCommLast, totalTasks);

                    abstractContainerCommunicator.report(reportCommunication);

                    lastReportTimeStamp = now;
                    mergedAllTgCommLast = mergedAllTgComm;
                }

                errorLimit.checkRecordLimit(mergedAllTgComm);

                if (mergedAllTgComm.getState() == State.SUCCEEDED) {
                    LOG.info("accomplished all task group.");
                    break;
                }

                if (isJobKilling(jobId)) {
                    dealKillingStat(abstractContainerCommunicator, totalTasks);
                } else if (mergedAllTgComm.getState() == State.FAILED) {
                    dealFailedStat(abstractContainerCommunicator, mergedAllTgComm.getThrowable());
                }

                Thread.sleep(jobSleepIntervalMs);
            }
        } catch (InterruptedException e) {
            LOG.error("捕获到InterruptedException异常!", e);
            throw DataXException.build(FrameworkErrorCode.RUNTIME_ERROR, e);
        }
    }

    /**
     * 是不是分布式调度这里有体现
     */
    protected abstract void startAllTaskGroup(List<Configuration> configurations);

    protected abstract void dealFailedStat(AbstractContainerCommunicator frameworkCollector, Throwable throwable);

    protected abstract void dealKillingStat(AbstractContainerCommunicator frameworkCollector, int totalTasks);

    private int calculateTaskCount(List<Configuration> taskGroupConfList) {
        int totalTasks = 0;
        for (Configuration taskGroupConfiguration : taskGroupConfList) {
            totalTasks += taskGroupConfiguration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT).size();
        }
        return totalTasks;
    }

//    private boolean isJobKilling(Long jobId) {
//        Result<Integer> jobInfo = DataxServiceUtil.getJobInfo(jobId);
//        return jobInfo.getData() == State.KILLING.value();
//    }

    protected boolean isJobKilling(Long jobId){
        return false;
    }
}
