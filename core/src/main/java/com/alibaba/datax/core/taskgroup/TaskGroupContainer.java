package com.alibaba.datax.core.taskgroup;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.container.AbstractContainer;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.communicator.TGContainerCommunicator;
import com.alibaba.datax.core.statistics.plugin.task.AbstractTaskPluginCollector;
import com.alibaba.datax.core.taskgroup.runner.AbstractRunner;
import com.alibaba.datax.core.taskgroup.runner.ReaderRunner;
import com.alibaba.datax.core.taskgroup.runner.WriterRunner;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.exchanger.BufferedRecordExchanger;
import com.alibaba.datax.core.transport.exchanger.BufferedRecordTransformerExchanger;
import com.alibaba.datax.core.transport.transformer.TransformerExecution;
import com.alibaba.datax.core.util.ClassUtil;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.Global;
import com.alibaba.datax.core.util.TransformerUtil;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.datax.common.constant.State;
import com.alibaba.fastjson.JSON;
import lombok.Getter;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Getter
public class TaskGroupContainer extends AbstractContainer {
    private static final Logger LOG = LoggerFactory.getLogger(TaskGroupContainer.class);

    /**
     * 当前taskGroup所属jobId
     */
    private long jobId;

    /**
     * 当前taskGroupId
     */
    private int taskGroupId;

    /**
     * 使用的channel类
     */
    private String channelClassName;

    /**
     * task收集器使用的类
     */
    private String taskCollectorClassName;

    private TaskMonitor taskMonitor = TaskMonitor.getInstance();

    public TaskGroupContainer(Configuration taskGroupConfig) {
        super(taskGroupConfig);

        switch (Global.mode) {
            case taskGroup:
            default:
        }

        abstractContainerCommunicator = new TGContainerCommunicator(configuration);

        jobId = configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        taskGroupId = configuration.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
        channelClassName = configuration.getString(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CLASS);
        taskCollectorClassName = configuration.getString(CoreConstant.DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_TASKCLASS);
    }

    @Override
    public void start() {
        try {
            // 状态check时间间隔应该较短，可以把任务及时分发到对应channel中
            int sleepIntervalInMillSec = configuration.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_SLEEPINTERVAL, 100);

            // 状态汇报时间间隔应该稍长,不要大量汇报
            long reportIntervalMs = configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_REPORTINTERVAL, 10000);

            // 获取channel数目
            int channelNumber = configuration.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL);

            int taskMaxRetryTimes = configuration.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASK_FAILOVER_MAXRETRYTIMES, 1);

            // 用来应对重试之间的时间过短
            long taskRetryIntervalMs = configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_TASK_FAILOVER_RETRYINTERVALINMSEC, 10000);

            long taskMxaxWaitShutdownMs = configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_TASK_FAILOVER_MAXWAITINMSEC, 60000);

            List<Configuration> contentElementConfList = configuration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);

            if (LOG.isDebugEnabled()) {
                LOG.debug("taskGroup:{} 的 task configs:{}", taskGroupId, JSON.toJSONString(contentElementConfList));
            }

            LOG.info(String.format("taskGroupId:%d start %d channels for %d tasks.", taskGroupId, channelNumber, contentElementConfList.size()));

            // standaloneTGContainerCommunicator
            abstractContainerCommunicator.addCommunication(contentElementConfList);

            Map<Integer, Configuration> taskId_ContentElementConfig = buildTaskConfigMap(contentElementConfList); //taskId与task配置

            List<Configuration> pendingTaskList = new LinkedList<>(contentElementConfList); // 待运行的task
            Map<Integer, TaskExecutor> taskId_failedTaskExecutor = new HashMap<>(channelNumber); // 失败的task
            List<TaskExecutor> runningTaskExecutorList = new ArrayList<>(channelNumber); // 正在运行的task

            Map<Integer, Long> taskId_startTime = new HashMap<>(); // 任务开始时间

            long lastReportTimeStamp = 0;
            Communication lastRoundTaskGroupComm = new Communication();

            while (true) {
                boolean hasFailedOrKilledTask = false;

                Map<Integer, Communication> taskId_communication = abstractContainerCommunicator.getCommunicationMap();
                a:
                for (Map.Entry<Integer, Communication> entry : taskId_communication.entrySet()) {
                    Integer taskId = entry.getKey();
                    Communication taskCommunication = entry.getValue();

                    if (!taskCommunication.isFinished()) {
                        continue;
                    }

                    TaskExecutor taskExecutor = removeTask(runningTaskExecutorList, taskId);
                    if (taskExecutor == null) {
                        continue;
                    }

                    taskMonitor.removeTask(taskId);

                    switch (taskCommunication.getState()) {
                        case FAILED:
                            taskId_failedTaskExecutor.put(taskId, taskExecutor);

                            // task是否支持failover && 重试次数未超过最大限制
                            if (taskExecutor.supportFailOver() && taskExecutor.getAttemptCount() < taskMaxRetryTimes) {
                                taskExecutor.shutdown(); // 关闭老的executor
                                abstractContainerCommunicator.resetCommunication(taskId); // 将task的状态重置

                                // 重新加入任务列表
                                pendingTaskList.add(taskId_ContentElementConfig.get(taskId));
                            } else {
                                hasFailedOrKilledTask = true;
                                break a;
                            }
                        case KILLED:
                            hasFailedOrKilledTask = true;
                            break a;
                        case SUCCEEDED:
                            Long taskStartTime = taskId_startTime.get(taskId);
                            if (taskStartTime != null) {
                                long usedTime = System.currentTimeMillis() - taskStartTime;

                                LOG.info("taskGroup:{} taskId:{} is success, used {} ms", taskGroupId, taskId, usedTime);
                                PerfRecord.addPerfRecord(taskGroupId, taskId, PerfRecord.PHASE.TASK_TOTAL, taskStartTime, usedTime * 1000L * 1000L);

                                taskId_startTime.remove(taskId);
                                taskId_ContentElementConfig.remove(taskId);
                            }
                    }
                }

                // 2.发现该taskGroup下taskExecutor的总状态失败则汇报错误
                if (hasFailedOrKilledTask) {
                    lastRoundTaskGroupComm = reportTaskGroupComm(lastRoundTaskGroupComm, contentElementConfList.size());
                    throw DataXException.build(FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, lastRoundTaskGroupComm.getThrowable());
                }

                // 3.有任务未执行，且正在运行的任务数小于最大通道限制
                Iterator<Configuration> iterator = pendingTaskList.iterator();
                while (iterator.hasNext() && runningTaskExecutorList.size() < channelNumber) {
                    Configuration contentElementConfig = iterator.next();

                    Integer taskId = contentElementConfig.getInt(CoreConstant.TASK_ID);
                    int retriedCount = 1;

                    TaskExecutor failedTaskExecutor = taskId_failedTaskExecutor.get(taskId);
                    if (failedTaskExecutor != null) { // 说明是之前失败过的了又再到了pending
                        retriedCount = failedTaskExecutor.getAttemptCount() + 1;

                        long now = System.currentTimeMillis();

                        long failedTime = failedTaskExecutor.getTimeStamp();
                        if (now - failedTime < taskRetryIntervalMs) {  // 未到等待时间，继续留在队列pending
                            continue;
                        }

                        if (!failedTaskExecutor.isShutdown()) { // 上趟失败的task仍未结束
                            if (now - failedTime > taskMxaxWaitShutdownMs) {
                                markCommunicationFailed(taskId);
                                reportTaskGroupComm(lastRoundTaskGroupComm, contentElementConfList.size());
                                throw DataXException.build(CommonErrorCode.WAIT_TIME_EXCEED, "task failover等待超时");
                            }

                            failedTaskExecutor.shutdown(); //再次尝试关闭
                            continue;
                        }

                        LOG.info("taskGroup:{} taskId:{} retriedCount:{} shutdown",
                                taskGroupId, taskId, failedTaskExecutor.getAttemptCount());
                    }

                    Configuration contentElementConfig0 = taskMaxRetryTimes > 1 ? contentElementConfig.clone() : contentElementConfig;
                    TaskExecutor taskExecutor = new TaskExecutor(contentElementConfig0, retriedCount);
                    taskId_startTime.put(taskId, System.currentTimeMillis());
                    taskExecutor.doStart();

                    iterator.remove();
                    runningTaskExecutorList.add(taskExecutor);

                    // 增加task到runTasks列表，因此在monitor里注册。
                    taskMonitor.registerTask(taskId, abstractContainerCommunicator.getCommunication(taskId));

                    taskId_failedTaskExecutor.remove(taskId);
                    LOG.info("taskGroup[{}] taskId[{}] attemptCount[{}] is started", taskGroupId, taskId, retriedCount);
                }

                // 4.任务列表为空，executor已结束, 搜集状态为success--->成功
                if (pendingTaskList.isEmpty() &&
                        isAllTaskDone(runningTaskExecutorList) &&
                        abstractContainerCommunicator.collect().getState() == State.SUCCEEDED) {

                    // 成功的情况下，也需要汇报一次。否则在任务结束非常快的情况下，采集的信息将会不准确
                    lastRoundTaskGroupComm = reportTaskGroupComm(lastRoundTaskGroupComm, contentElementConfList.size());

                    LOG.info("taskGroup:{} completed all tasks.", taskGroupId);
                    break;
                }

                // 5.如果当前时间已经超出汇报时间的interval，那么我们需要马上汇报
                long now = System.currentTimeMillis();
                if (now - lastReportTimeStamp > reportIntervalMs) {
                    lastRoundTaskGroupComm = reportTaskGroupComm(lastRoundTaskGroupComm, contentElementConfList.size());

                    lastReportTimeStamp = now;

                    // taskMonitor对于正在运行的task，每reportIntervalInMillSec进行检查
                    for (TaskExecutor taskExecutor : runningTaskExecutorList) {
                        taskMonitor.report(taskExecutor.taskId, abstractContainerCommunicator.getCommunication(taskExecutor.taskId));
                    }

                }

                Thread.sleep(sleepIntervalInMillSec);
            }

            // 6.最后还要汇报1趟
            reportTaskGroupComm(lastRoundTaskGroupComm, contentElementConfList.size());
        } catch (Throwable e) {
            Communication nowTaskGroupContainerCommunication = abstractContainerCommunicator.collect();

            if (nowTaskGroupContainerCommunication.getThrowable() == null) {
                nowTaskGroupContainerCommunication.setThrowable(e);
            }
            nowTaskGroupContainerCommunication.setState(State.FAILED);
            abstractContainerCommunicator.report(nowTaskGroupContainerCommunication);

            throw DataXException.build(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        } finally {
            if (!PerfTrace.getInstance().isJob()) {
                //最后打印cpu的平均消耗，GC的统计
                VMInfo vmInfo = VMInfo.getVmInfo();
                if (vmInfo != null) {
                    vmInfo.getDelta(false);
                    LOG.info(vmInfo.totalString());
                }

                LOG.info(PerfTrace.getInstance().summarizeNoException());
            }
        }
    }

    private Map<Integer, Configuration> buildTaskConfigMap(List<Configuration> contentConfigList) {
        Map<Integer, Configuration> taskId_ContentConfig = new HashMap<>();
        for (Configuration taskConfig : contentConfigList) {
            int taskId = taskConfig.getInt(CoreConstant.TASK_ID);
            taskId_ContentConfig.put(taskId, taskConfig);
        }
        return taskId_ContentConfig;
    }

    private List<Configuration> buildRemainTasks(List<Configuration> configurations) {
        return new LinkedList<>(configurations);
    }

    private TaskExecutor removeTask(List<TaskExecutor> taskList, int taskId) {
        Iterator<TaskExecutor> iterator = taskList.iterator();
        while (iterator.hasNext()) {
            TaskExecutor taskExecutor = iterator.next();
            if (taskExecutor.getTaskId() == taskId) {
                iterator.remove();
                return taskExecutor;
            }
        }
        return null;
    }

    private boolean isAllTaskDone(List<TaskExecutor> taskList) {
        for (TaskExecutor taskExecutor : taskList) {
            if (!taskExecutor.isTaskFinished()) {
                return false;
            }
        }
        return true;
    }

    private Communication reportTaskGroupComm(Communication mergedAllTaskCommOld, int taskCount) {
        Communication mergedAllTaskCommNew = abstractContainerCommunicator.collect();
        mergedAllTaskCommNew.setTimestamp(System.currentTimeMillis());
        Communication reportCommunication = CommunicationTool.getReportComm(mergedAllTaskCommNew, mergedAllTaskCommOld, taskCount);
        // 更新到了 taskGroupId_communication
        abstractContainerCommunicator.report(reportCommunication);
        return reportCommunication;
    }

    private void markCommunicationFailed(Integer taskId) {
        Communication communication = abstractContainerCommunicator.getCommunication(taskId);
        communication.setState(State.FAILED);
    }

    /**
     * TaskExecutor是一个完整task的执行器
     * 其中包括1：1的reader和writer
     */
    private class TaskExecutor {
        private Configuration contentElementConfig;

        private int taskId;

        private int attemptCount;

        private Channel channel;

        private Thread readerThread;

        private Thread writerThread;

        private ReaderRunner readerRunner;

        private WriterRunner writerRunner;

        /**
         * 该处的taskCommunication在多处用到：
         * 1. channel
         * 2. readerRunner和writerRunner
         * 3. reader和writer的taskPluginCollector
         */
        private Communication taskCommunication;

        public TaskExecutor(Configuration contentElementConfig, int hasAlreadyRetriedCount) {
            // 获取该taskExecutor的配置
            this.contentElementConfig = contentElementConfig;

            Validate.isTrue(null != contentElementConfig.getConfig(CoreConstant.JOB_READER)
                            && null != contentElementConfig.getConfig(CoreConstant.JOB_WRITER),
                    "[reader|writer]的插件参数不能为空!");

            // taskId
            taskId = contentElementConfig.getInt(CoreConstant.TASK_ID);
            this.attemptCount = hasAlreadyRetriedCount;

            // 由taskId得到该taskExecutor的Communication
            // 要传给readerRunner和writerRunner，同时要传给channel作统计用
            taskCommunication = abstractContainerCommunicator.getCommunication(taskId);
            Validate.notNull(taskCommunication, String.format("taskId[%d]的Communication没有注册过", taskId));

            channel = ClassUtil.instantiate(channelClassName, Channel.class, configuration);
            channel.setCommunication(taskCommunication);

            // 获取transformer的参数
            List<TransformerExecution> transformerInfoExecs = TransformerUtil.buildTransformerInfo(this.contentElementConfig);

            // 生成writerThread
            writerRunner = (WriterRunner) generateRunner(PluginType.WRITER);
            writerThread = new Thread(writerRunner, String.format("%d-%d-%d-writer", jobId, taskGroupId, taskId));
            // 通过设置thread的contextClassLoader，即可实现同步和主程序不通的加载器
            writerThread.setContextClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, contentElementConfig.getString(CoreConstant.JOB_WRITER_NAME)));

            // 生成readerThread
            readerRunner = (ReaderRunner) generateRunner(PluginType.READER, transformerInfoExecs);
            readerThread = new Thread(readerRunner, String.format("%d-%d-%d-reader", jobId, taskGroupId, this.taskId));
            // 通过设置thread的contextClassLoader，即可实现同步和主程序不通的加载器
            readerThread.setContextClassLoader(LoadUtil.getJarLoader(PluginType.READER, contentElementConfig.getString(CoreConstant.JOB_READER_NAME)));
        }

        public void doStart() {
            writerThread.start();
            // reader没有起来，writer不可能结束
            if (!writerThread.isAlive() || taskCommunication.getState() == State.FAILED) {
                throw DataXException.build(FrameworkErrorCode.RUNTIME_ERROR, taskCommunication.getThrowable());
            }

            readerThread.start();
            // 这里reader可能很快结束
            if (!readerThread.isAlive() && taskCommunication.getState() == State.FAILED) {
                // 这里有可能出现Reader线上启动即挂情况 对于这类情况 需要立刻抛出异常
                throw DataXException.build(FrameworkErrorCode.RUNTIME_ERROR, taskCommunication.getThrowable());
            }

        }


        private AbstractRunner generateRunner(PluginType pluginType) {
            return generateRunner(pluginType, null);
        }

        private AbstractRunner generateRunner(PluginType pluginType,
                                              List<TransformerExecution> transformerInfoExecs) {
            AbstractRunner abstractRunner;
            TaskPluginCollector taskPluginCollector;

            switch (pluginType) {
                case READER:
                    abstractRunner = LoadUtil.loadPluginRunner(pluginType, contentElementConfig.getString(CoreConstant.JOB_READER_NAME));
                    abstractRunner.setJobConf(contentElementConfig.getConfig(CoreConstant.JOB_READER_PARAMETER));

                    taskPluginCollector = ClassUtil.instantiate(
                            taskCollectorClassName,
                            AbstractTaskPluginCollector.class,
                            configuration,
                            taskCommunication,
                            PluginType.READER);

                    RecordSender recordSender;
                    if (transformerInfoExecs != null && transformerInfoExecs.size() > 0) {
                        recordSender = new BufferedRecordTransformerExchanger(taskGroupId, taskId, channel, taskCommunication, taskPluginCollector, transformerInfoExecs);
                    } else {
                        recordSender = new BufferedRecordExchanger(channel, taskPluginCollector);
                    }
                    ((ReaderRunner) abstractRunner).setRecordSender(recordSender);

                    // 设置taskPlugin的collector，用来处理脏数据和job/task通信
                    abstractRunner.setTaskPluginCollector(taskPluginCollector);
                    break;
                case WRITER:
                    abstractRunner = LoadUtil.loadPluginRunner(pluginType, contentElementConfig.getString(CoreConstant.JOB_WRITER_NAME));
                    abstractRunner.setJobConf(contentElementConfig.getConfig(CoreConstant.JOB_WRITER_PARAMETER));

                    taskPluginCollector = ClassUtil.instantiate(
                            taskCollectorClassName,
                            AbstractTaskPluginCollector.class,
                            configuration,
                            taskCommunication,
                            PluginType.WRITER);

                    ((WriterRunner) abstractRunner).setRecordReceiver(new BufferedRecordExchanger(channel, taskPluginCollector));

                    // 设置taskPlugin的collector，用来处理脏数据和job/task通信
                    abstractRunner.setTaskPluginCollector(taskPluginCollector);
                    break;
                default:
                    throw DataXException.build(FrameworkErrorCode.ARGUMENT_ERROR, "Cant generateRunner for:" + pluginType);
            }

            abstractRunner.setTaskGroupId(taskGroupId);
            abstractRunner.setTaskId(taskId);
            abstractRunner.setCommunication(taskCommunication);

            return abstractRunner;
        }

        // 检查任务是否结束
        private boolean isTaskFinished() {
            // 如果reader 或 writer没有完成工作，那么直接返回工作没有完成
            if (readerThread.isAlive() || writerThread.isAlive()) {
                return false;
            }

            if (taskCommunication == null || !taskCommunication.isFinished()) {
                return false;
            }

            return true;
        }

        private int getTaskId() {
            return taskId;
        }

        private long getTimeStamp() {
            return taskCommunication.getTimestamp();
        }

        private int getAttemptCount() {
            return attemptCount;
        }

        private boolean supportFailOver() {
            return writerRunner.supportFailOver();
        }

        private void shutdown() {
            writerRunner.shutdown();
            readerRunner.shutdown();
            if (writerThread.isAlive()) {
                writerThread.interrupt();
            }
            if (readerThread.isAlive()) {
                readerThread.interrupt();
            }
        }

        private boolean isShutdown() {
            return !readerThread.isAlive() && !writerThread.isAlive();
        }
    }
}
