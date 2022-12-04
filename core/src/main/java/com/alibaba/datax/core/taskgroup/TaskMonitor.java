package com.alibaba.datax.core.taskgroup;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.common.constant.State;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by liqiang on 15/7/23.
 */
public class TaskMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(TaskMonitor.class);
    private static final TaskMonitor instance = new TaskMonitor();

    private static long EXPIRED_TIME = 172800 * 1000;
    private ConcurrentHashMap<Integer, TaskCommunication> taskId_taskCommunication = new ConcurrentHashMap<>();

    private TaskMonitor() {
    }

    public static TaskMonitor getInstance() {
        return instance;
    }

    public void registerTask(Integer taskId, Communication communication) {
        //如果task已经finish，直接返回
        if (communication.isFinished()) {
            return;
        }

        taskId_taskCommunication.putIfAbsent(taskId, new TaskCommunication(taskId, communication));
    }

    public void removeTask(Integer taskid) {
        taskId_taskCommunication.remove(taskid);
    }

    public void report(Integer taskid, Communication communication) {
        //如果task已经finish，直接返回
        if (communication.isFinished()) {
            return;
        }

        if (!taskId_taskCommunication.containsKey(taskid)) {
            LOG.warn("unexpected: taskid({}) missed.", taskid);
            taskId_taskCommunication.putIfAbsent(taskid, new TaskCommunication(taskid, communication));
        } else {
            taskId_taskCommunication.get(taskid).report(communication);
        }
    }

    public TaskCommunication getTaskCommunication(Integer taskid) {
        return taskId_taskCommunication.get(taskid);
    }

    @Getter
    public static class TaskCommunication {
        private Integer taskId;

        // 记录最后更新的communication
        private long lastAllReadRecords;

        // 只有第1趟，或者统计变更时才会更新TS
        private long lastUpdateComunicationTS;

        private long ttl;

        private TaskCommunication(Integer taskId, Communication communication) {
            this.taskId = taskId;
            lastAllReadRecords = CommunicationTool.getTotalReadRecordCount(communication);
            ttl = System.currentTimeMillis();
            lastUpdateComunicationTS = ttl;
        }

        public void report(Communication communication) {
            ttl = System.currentTimeMillis();

            // 采集的数量增长，则变更当前记录, 优先判断这个条件，因为目的是不卡住，而不是expired
            if (CommunicationTool.getTotalReadRecordCount(communication) > lastAllReadRecords) {
                lastAllReadRecords = CommunicationTool.getTotalReadRecordCount(communication);
                lastUpdateComunicationTS = ttl;
            } else if (isExpired(lastUpdateComunicationTS)) {
                communication.setState(State.FAILED);
                communication.setTimestamp(ttl);
                communication.setThrowable(DataXException.build(CommonErrorCode.TASK_HUNG_EXPIRED,
                        String.format("task(%s) hung expired [allReadRecord(%s), elased(%s)]",
                                taskId, lastAllReadRecords, (ttl - lastUpdateComunicationTS))));
            }


        }

        private boolean isExpired(long lastUpdateComunicationTS) {
            return System.currentTimeMillis() - lastUpdateComunicationTS > EXPIRED_TIME;
        }
    }
}
