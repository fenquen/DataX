package com.alibaba.datax.core.statistics.plugin;

import com.alibaba.datax.common.plugin.JobPluginCollector;
import com.alibaba.datax.core.statistics.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.communication.Communication;

import java.util.List;
import java.util.Map;

/**
 * Created by jingxing on 14-9-9.
 */
public final class DefaultJobPluginCollector implements JobPluginCollector {
    private AbstractContainerCommunicator abstractContainerCommunicator;

    public DefaultJobPluginCollector(AbstractContainerCommunicator containerCollector) {
        this.abstractContainerCommunicator = containerCollector;
    }

    @Override
    public Map<String, List<String>> getMessage() {
        Communication totalCommunication = abstractContainerCommunicator.collect();
        return totalCommunication.getId_messageList();
    }

    @Override
    public List<String> getMessage(String key) {
        Communication totalCommunication = abstractContainerCommunicator.collect();
        return totalCommunication.getMessageListByKey(key);
    }
}
