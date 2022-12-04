package com.alibaba.datax.core.statistics.collector;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.util.container.CoreConstant;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class Collector {
    public Map<Integer, Communication> taskId_communication = new ConcurrentHashMap<>();

    /**
     * 管理的是task group这个level上的communication 会有不同的实现
     * 有分布式和不是分布是的实现
     */
    public Communication collectTaskGroup() {
        return LocalTGCommunicationManager.getMergedTgComm();
    }

    public void addTGCommunication(List<Configuration> taskGroupConfList) {
        for (Configuration config : taskGroupConfList) {
            int taskGroupId = config.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            LocalTGCommunicationManager.add(taskGroupId, new Communication());
        }
    }

    public Communication getTGCommunication(Integer taskGroupId) {
        return LocalTGCommunicationManager.get(taskGroupId);
    }
}
