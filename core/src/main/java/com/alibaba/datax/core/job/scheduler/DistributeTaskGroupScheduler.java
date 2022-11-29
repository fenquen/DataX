package com.alibaba.datax.core.job.scheduler;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communicator.AbstractContainerCommunicator;

import java.util.List;

public class DistributeTaskGroupScheduler extends AbstractTaskGroupScheduler {
    public DistributeTaskGroupScheduler(AbstractContainerCommunicator abstractContainerCommunicator) {
        super(abstractContainerCommunicator);
    }

    @Override
    protected void startAllTaskGroup(List<Configuration> configurations) {

    }

    @Override
    protected void dealFailedStat(AbstractContainerCommunicator frameworkCollector, Throwable throwable) {

    }

    @Override
    protected void dealKillingStat(AbstractContainerCommunicator frameworkCollector, int totalTasks) {

    }
}
