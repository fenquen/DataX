package com.alibaba.datax.core.job.scheduler.processinner;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.scheduler.AbstractScheduler;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.taskgroup.runner.TaskGroupContainerRunner;
import com.alibaba.datax.core.util.FrameworkErrorCode;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class ProcessInnerScheduler extends AbstractScheduler {

    private ExecutorService taskGroupContainerExecutorService;

    public ProcessInnerScheduler(AbstractContainerCommunicator containerCommunicator) {
        super(containerCommunicator);
    }

    @Override
    public void startAllTaskGroup(List<Configuration> taskGroupConfigList) {
        taskGroupContainerExecutorService = Executors.newFixedThreadPool(taskGroupConfigList.size());

        for (Configuration taskGroupConfig : taskGroupConfigList) {
            TaskGroupContainerRunner taskGroupContainerRunner = new TaskGroupContainerRunner(new TaskGroupContainer(taskGroupConfig));
            taskGroupContainerExecutorService.execute(taskGroupContainerRunner);
        }

        taskGroupContainerExecutorService.shutdown();
    }

    @Override
    public void dealFailedStat(AbstractContainerCommunicator frameworkCollector, Throwable throwable) {
        taskGroupContainerExecutorService.shutdownNow();
        throw DataXException.build(FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, throwable);
    }


    @Override
    public void dealKillingStat(AbstractContainerCommunicator frameworkCollector, int totalTasks) {
        // 通过进程退出返回码标示状态
        taskGroupContainerExecutorService.shutdownNow();
        throw DataXException.build(FrameworkErrorCode.KILLED_EXIT_VALUE, "job killed status");
    }

    private TaskGroupContainerRunner newTaskGroupContainerRunner(Configuration taskGroupConfig) {
        return new TaskGroupContainerRunner(new TaskGroupContainer(taskGroupConfig));
    }
}
