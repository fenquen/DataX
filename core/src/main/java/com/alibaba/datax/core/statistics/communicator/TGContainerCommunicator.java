package com.alibaba.datax.core.statistics.communicator;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.common.constant.State;
import org.apache.commons.lang.Validate;

import java.util.List;
import java.util.Map;

/**
 * 管理的是各个的task
 */
public class TGContainerCommunicator extends AbstractContainerCommunicator {

    protected long jobId;

    /**
     * 由于taskGroupContainer是进程内部调度
     * 其registerCommunication()，getCommunication()，
     * getCommunications()，collect()等方法是一致的
     * 所有TG的Collector都是ProcessInnerCollector
     */
    protected int taskGroupId;

    public TGContainerCommunicator(Configuration configuration) {
        super(configuration);
        jobId = configuration.getInt(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        taskGroupId = configuration.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
    }

    @Override
    public void addCommunication(List<Configuration> contentElementConfList) {
        for (Configuration contentElement : contentElementConfList) {
            collector.taskId_communication.put(contentElement.getInt(CoreConstant.TASK_ID), new Communication());
        }
    }

    /**
     * 收集合并了全部的task的communication便是task group的
     */
    @Override
    public final Communication collect() {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for (Communication taskCommunication : collector.taskId_communication.values()) {
            communication.mergeFrom(taskCommunication);
        }

        return communication;
    }

    @Override
    public final Communication getCommunication(Integer taskId) {
        Validate.isTrue(taskId >= 0, "注册的taskId不能小于0");
        return collector.taskId_communication.get(taskId);
    }

    @Override
    public final Map<Integer, Communication> getCommunicationMap() {
        return collector.taskId_communication;
    }

    @Override
    public void report(Communication communication) {
        reporter.reportTGCommunication(taskGroupId, communication);
    }
}
