package com.alibaba.datax.core.statistics.collector;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.util.container.CoreConstant;

import java.util.List;

public class LocalCollector extends AbstractCollector {

    @Override
    public void registerTGCommunication(List<Configuration> taskGroupConfigList) {
        for (Configuration config : taskGroupConfigList) {
            int taskGroupId = config.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            LocalTGCommunicationManager.register(taskGroupId, new Communication());
        }
    }

    @Override
    public Communication collectTaskGroup() {
        return LocalTGCommunicationManager.getJobCommunication();
    }
}
