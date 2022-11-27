package com.alibaba.datax.core.statistics.communication;

import com.alibaba.datax.dataxservice.face.domain.enums.State;
import org.apache.commons.lang3.Validate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class LocalTGCommunicationManager {
    public static final Map<Integer, Communication> taskGroupId_communication = new ConcurrentHashMap<>();

    public static void registerTaskGroupCommunication(int taskGroupId, Communication communication) {
        taskGroupId_communication.put(taskGroupId, communication);
    }

    public static Communication getJobCommunication() {
        Communication communication = new Communication();
        communication.setState(State.SUCCEEDED);

        for (Communication taskGroupCommunication : taskGroupId_communication.values()) {
            communication.mergeFrom(taskGroupCommunication);
        }

        return communication;
    }


    public static Communication getTaskGroupCommunication(int taskGroupId) {
        Validate.isTrue(taskGroupId >= 0, "taskGroupId不能小于0");

        return taskGroupId_communication.get(taskGroupId);
    }

    public static void updateTaskGroupCommunication(int taskGroupId, Communication communication) {
        Validate.isTrue(taskGroupId_communication.containsKey(taskGroupId),
                String.format("没有注册taskGroupId[%d]的Communication无法更新该taskGroup的信息", taskGroupId));

        taskGroupId_communication.put(taskGroupId, communication);
    }

    public static void clear() {
        taskGroupId_communication.clear();
    }
}