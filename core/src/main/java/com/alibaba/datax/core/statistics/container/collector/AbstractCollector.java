package com.alibaba.datax.core.statistics.container.collector;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.dataxservice.face.domain.enums.State;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
public abstract class AbstractCollector {
    public Map<Integer, Communication> taskId_communication = new ConcurrentHashMap<>();

    protected Long jobId;

    public void registerTGCommunication(List<Configuration> taskGroupConfigList) {
        for (Configuration config : taskGroupConfigList) {
            int taskGroupId = config.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            LocalTGCommunicationManager.registerTaskGroupCommunication(taskGroupId, new Communication());
        }
    }

    public void registerTaskCommunication(List<Configuration> contentElementList) {
        for (Configuration contentElement : contentElementList) {
            int taskId = contentElement.getInt(CoreConstant.TASK_ID);
            taskId_communication.put(taskId, new Communication());
        }
    }

    public Communication collectFromTask() {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for (Communication taskCommunication : taskId_communication.values()) {
            communication.mergeFrom(taskCommunication);
        }

        return communication;
    }

    public abstract Communication collectFromTaskGroup();

    public Communication getTGCommunication(Integer taskGroupId) {
        return LocalTGCommunicationManager.getTaskGroupCommunication(taskGroupId);
    }

    public Communication getTaskCommunication(Integer taskId) {
        return taskId_communication.get(taskId);
    }
}
