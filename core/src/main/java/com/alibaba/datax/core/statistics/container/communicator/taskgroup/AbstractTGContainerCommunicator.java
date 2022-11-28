package com.alibaba.datax.core.statistics.container.communicator.taskgroup;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.container.collector.ProcessInnerCollector;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.apache.commons.lang.Validate;

import java.util.List;
import java.util.Map;

/**
 * 该类是用于处理 taskGroupContainer 的 communication 的收集汇报的父类
 * 主要是 taskCommunicationMap 记录了 taskExecutor 的 communication 属性
 */
public abstract class AbstractTGContainerCommunicator extends AbstractContainerCommunicator {

    protected long jobId;

    /**
     * 由于taskGroupContainer是进程内部调度
     * 其registerCommunication()，getCommunication()，
     * getCommunications()，collect()等方法是一致的
     * 所有TG的Collector都是ProcessInnerCollector
     */
    protected int taskGroupId;

    public AbstractTGContainerCommunicator(Configuration configuration) {
        super(configuration);
        jobId = configuration.getInt(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        abstractCollector = new ProcessInnerCollector(this.jobId);
        taskGroupId = configuration.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
    }

    @Override
    public void registerCommunication(List<Configuration> elementContentConfigList) {
        abstractCollector.registerTaskCommunication(elementContentConfigList);
    }

    @Override
    public final Communication collect() {
        return abstractCollector.collectTask();
    }

    @Override
    public final State collectState() {
        return collect().getState();
    }

    @Override
    public final Communication getCommunication(Integer taskId) {
        Validate.isTrue(taskId >= 0, "注册的taskId不能小于0");
        return abstractCollector.getTaskCommunication(taskId);
    }

    @Override
    public final Map<Integer, Communication> getCommunicationMap() {
        return abstractCollector.taskId_communication;
    }

}
