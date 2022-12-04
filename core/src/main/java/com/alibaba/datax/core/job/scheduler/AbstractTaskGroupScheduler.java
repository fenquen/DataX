package com.alibaba.datax.core.job.scheduler;


import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.communicator.JobContainerCommunicator;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.taskgroup.runner.TaskGroupContainerRunner;
import com.alibaba.datax.core.util.ErrorRecordChecker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.common.constant.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 都是在源头端的
 */
public abstract class AbstractTaskGroupScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTaskGroupScheduler.class);

    protected ExecutorService executorService;

    private ErrorRecordChecker errorLimit;

    private JobContainerCommunicator jobContainerCommunicator;

    private Long jobId;

    public Long getJobId() {
        return jobId;
    }

    public AbstractTaskGroupScheduler(JobContainerCommunicator jobContainerCommunicator) {
        this.jobContainerCommunicator = jobContainerCommunicator;
    }

    public void schedule(List<Configuration> taskGroupConfigList) {
        int jobReportIntervalMs = taskGroupConfigList.get(0).getInt(CoreConstant.DATAX_CORE_CONTAINER_JOB_REPORTINTERVAL, 30000);
        int jobSleepIntervalMs = taskGroupConfigList.get(0).getInt(CoreConstant.DATAX_CORE_CONTAINER_JOB_SLEEPINTERVAL, 10000);

        jobId = taskGroupConfigList.get(0).getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);

        errorLimit = new ErrorRecordChecker(taskGroupConfigList.get(0));

        jobContainerCommunicator.addCommunication(taskGroupConfigList);

        int totalTasks = calculateTaskCount(taskGroupConfigList);

        // 是不是分布式这里有体现
        startAllTaskGroup(taskGroupConfigList);

        Communication mergedAllTgCommLastRound = new Communication();

        long lastReportTimeStamp = System.currentTimeMillis();

        try {
            while (true) {
                // 是distribute要点 如何收集在远端调用的task group的状态
                Communication mergedAllTgComm = jobContainerCommunicator.collect(); // collect的是 task group的
                mergedAllTgComm.setTimestamp(System.currentTimeMillis());

                // 汇报周期
                long now = System.currentTimeMillis();

                if (now - lastReportTimeStamp > jobReportIntervalMs) {
                    Communication reportCommunication =
                            CommunicationTool.getReportComm(mergedAllTgComm, mergedAllTgCommLastRound, totalTasks);

                    jobContainerCommunicator.report(reportCommunication);

                    lastReportTimeStamp = now;
                    mergedAllTgCommLastRound = mergedAllTgComm;
                }

                errorLimit.checkRecordLimit(mergedAllTgComm);

                if (mergedAllTgComm.getState() == State.SUCCEEDED) {
                    LOG.info("accomplished all task group.");
                    break;
                }

                if (isJobKilling(jobId)) {
                    dealKillingStat(jobContainerCommunicator, totalTasks);
                } else if (mergedAllTgComm.getState() == State.FAILED) {
                    dealFailedStat(jobContainerCommunicator, mergedAllTgComm.getThrowable());
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
    protected abstract void startAllTaskGroup(List<Configuration> taskGroupConfList);

    protected abstract void dealFailedStat(AbstractContainerCommunicator abstractContainerCommunicator, Throwable throwable);

    protected abstract void dealKillingStat(AbstractContainerCommunicator abstractContainerCommunicator, int totalTasks);

    private int calculateTaskCount(List<Configuration> taskGroupConfList) {
        int totalTasks = 0;
        for (Configuration taskGroupConfiguration : taskGroupConfList) {
            totalTasks += taskGroupConfiguration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT).size();
        }
        return totalTasks;
    }

    protected void scheduleLocally(List<Configuration> taskGroupConfList) {
        executorService = Executors.newFixedThreadPool(taskGroupConfList.size());

        for (Configuration taskGroupConfig : taskGroupConfList) {
            TaskGroupContainerRunner taskGroupContainerRunner = new TaskGroupContainerRunner(new TaskGroupContainer(taskGroupConfig));
            executorService.execute(taskGroupContainerRunner);
        }

        executorService.shutdown();
    }

//    private boolean isJobKilling(Long jobId) {
//        Result<Integer> jobInfo = DataxServiceUtil.getJobInfo(jobId);
//        return jobInfo.getData() == State.KILLING.value();
//    }

    protected boolean isJobKilling(Long jobId) {
        return false;
    }
}
