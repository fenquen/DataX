package com.alibaba.datax.core.statistics.communication;

import com.alibaba.datax.common.constant.State;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class LocalTGCommunicationManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalTGCommunicationManager.class);

    public static final Map<Integer, Communication> taskGroupId_communication = new ConcurrentHashMap<>();

    /**
     * 融合了全部的task group 级别的communication 作为总的communication
     * 收集各个的task group的communication的时候需要检视更新时间
     */
    public static Communication getMergedTgComm() {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        List<String> timeoutNodeHostList =
                communication.taskGroupState_taskGroupNodeHostList.computeIfAbsent(State.TASK_GROUP_TIME_OUT, state -> new ArrayList<>());

        for (Communication taskGroupCommunication : taskGroupId_communication.values()) {
            List<String> taskGroupNodeHostList =
                    communication.taskGroupState_taskGroupNodeHostList.computeIfAbsent(taskGroupCommunication.getState(), state -> new ArrayList<>());

            taskGroupNodeHostList.add(taskGroupCommunication.nodeHost);

            // 正在运行的
            if (!taskGroupCommunication.isFinished()) {
                // 要是执行task group的长时间没有上报了
                if (70000 < communication.getTimestamp() - taskGroupCommunication.getTimestamp()) {
                    // taskGroupCommunication最好是直接有相应的执行的node信息
                    taskGroupNodeHostList.add(taskGroupCommunication.nodeHost);
                }
            }

            communication.mergeFrom(taskGroupCommunication);
        }

        // timeout的优先级低
        if (communication.isRunning()) {
            if (CollectionUtils.isNotEmpty(timeoutNodeHostList)) {
                communication.setState(State.TASK_GROUP_TIME_OUT);
            }
        }

        return communication;
    }

    public static void add(int taskGroupId, Communication communication) {
        taskGroupId_communication.put(taskGroupId, communication);
    }

    public static Communication get(int taskGroupId) {
        Validate.isTrue(taskGroupId >= 0, "taskGroupId不能小于0");
        return taskGroupId_communication.get(taskGroupId);
    }

    /**
     * reporter用的
     */
    public static void update(int taskGroupId, Communication communication) {
        if (!taskGroupId_communication.containsKey(taskGroupId)) {
            LOGGER.info("taskGroupId {} 的Communication是不在的", taskGroupId);
            return;
        }

        taskGroupId_communication.put(taskGroupId, communication);
    }

    public static void clear() {
        taskGroupId_communication.clear();
    }
}