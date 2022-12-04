package com.alibaba.datax.core.job.scheduler;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.communicator.JobContainerCommunicator;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.taskgroup.runner.TaskGroupContainerRunner;
import com.alibaba.datax.core.util.FrameworkErrorCode;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LocalTaskGroupScheduler extends AbstractTaskGroupScheduler {

    public LocalTaskGroupScheduler(JobContainerCommunicator containerCommunicator) {
        super(containerCommunicator);
    }

    @Override
    public void startAllTaskGroup(List<Configuration> taskGroupConfList) {
        scheduleLocally(taskGroupConfList);
    }

    @Override
    public void dealFailedStat(AbstractContainerCommunicator abstractContainerCommunicator, Throwable throwable) {
        executorService.shutdownNow();
        throw DataXException.build(FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, throwable);
    }

    @Override
    public void dealKillingStat(AbstractContainerCommunicator abstractContainerCommunicator, int totalTasks) {
        // 通过进程退出返回码标示状态
        executorService.shutdownNow();
        throw DataXException.build(FrameworkErrorCode.KILLED_EXIT_VALUE);
    }
}
