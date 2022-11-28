package com.alibaba.datax.core.job;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractJobPlugin;
import com.alibaba.datax.common.plugin.JobPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.StrUtil;
import com.alibaba.datax.core.AbstractContainer;
import com.alibaba.datax.core.Engine;
import com.alibaba.datax.core.container.util.HookInvoker;
import com.alibaba.datax.core.container.util.JobAssignUtil;
import com.alibaba.datax.core.job.scheduler.AbstractScheduler;
import com.alibaba.datax.core.job.scheduler.processinner.StandAloneScheduler;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.container.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.container.communicator.job.StandAloneJobContainerCommunicator;
import com.alibaba.datax.core.statistics.plugin.DefaultJobPluginCollector;
import com.alibaba.datax.core.util.ErrorRecordChecker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.ClassLoaderSwapper;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.datax.dataxservice.face.domain.enums.ExecuteMode;
import com.alibaba.fastjson.JSON;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jingxing on 14-8-24.
 * <p/>
 * job实例运行在jobContainer容器中，它是所有任务的master，负责初始化、拆分、调度、运行、回收、监控和汇报
 * 但它并不做实际的数据同步操作
 */
public class JobContainer extends AbstractContainer {
    private static final Logger LOG = LoggerFactory.getLogger(JobContainer.class);

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper.newCurrentThreadClassLoaderSwapper();

    private long jobId;

    private String readerPluginName;

    private String writerPluginName;

    /**
     * reader和writer jobContainer的实例
     */
    private Reader.Job jobReader;

    private Writer.Job jobWriter;

    private Configuration userConf;

    private long startTimeStamp;

    private long endTimeStamp;

    private long startTransferTimeStamp;

    private long endTransferTimeStamp;

    private int needChannelNumber;

    private int splitCount = 1;

    private ErrorRecordChecker errorLimit;

    public JobContainer(Configuration configuration) {
        super(configuration);
        errorLimit = new ErrorRecordChecker(configuration);
    }

    /**
     * jobContainer主要负责的工作全部在start()里面，包括init、prepare、split、scheduler、
     * post以及destroy和statistics
     */
    @Override
    public void start() {
        LOG.info("DataX jobContainer starts job.");

        boolean hasException = false;
        boolean isDryRun = false;
        try {
            startTimeStamp = System.currentTimeMillis();
            isDryRun = configuration.getBool(CoreConstant.DATAX_JOB_SETTING_DRYRUN, false);
            if (isDryRun) {
                LOG.info("jobContainer starts to do preCheck");
                preCheck();
            } else {
                userConf = configuration.clone();

                LOG.info("jobContainer starts to do preHandle");
                preHandle();

                LOG.info("jobContainer starts to do init");
                init();

                LOG.info("jobContainer starts to do prepare");
                prepare();

                LOG.info("jobContainer starts to do split");
                splitCount = split(); // 切分后生成的多个的contentElement还是落地在当前的config的content

                LOG.info("jobContainer starts to do schedule");
                schedule();

                LOG.info("jobContainer starts to do post");
                post();

                LOG.info("jobContainer starts to do postHandle");
                postHandle();

                LOG.info("DataX jobId [{}] completed successfully", jobId);

                invokeHooks();
            }
        } catch (Throwable e) {
            LOG.error("Exception when job run", e);

            hasException = true;

            if (e instanceof OutOfMemoryError) {
                destroy();
                System.gc();
            }


            if (abstractContainerCommunicator== null) {
                // 由于 containerCollector 是在 scheduler() 中初始化的，所以当在 scheduler() 之前出现异常时，需要在此处对 containerCollector 进行初始化
                abstractContainerCommunicator = new StandAloneJobContainerCommunicator(configuration);
            }

            Communication communication = getAbstractContainerCommunicator().collect();
            // 汇报前的状态，不需要手动进行设置
            // communication.setState(State.FAILED);
            communication.setThrowable(e);
            communication.setTimestamp(endTimeStamp);

            Communication tempComm = new Communication();
            tempComm.setTimestamp(startTransferTimeStamp);

            Communication reportCommunication = CommunicationTool.getReportComm(communication, tempComm, splitCount);
            abstractContainerCommunicator.report(reportCommunication);

            throw DataXException.build(FrameworkErrorCode.RUNTIME_ERROR, e);
        } finally {
            if (!isDryRun) {
                destroy();
                endTimeStamp = System.currentTimeMillis();
                if (!hasException) {
                    //最后打印cpu的平均消耗，GC的统计
                    VMInfo vmInfo = VMInfo.getVmInfo();
                    if (vmInfo != null) {
                        vmInfo.getDelta(false);
                        LOG.info(vmInfo.totalString());
                    }

                    LOG.info(PerfTrace.getInstance().summarizeNoException());
                    logStatistics();
                }
            }
        }
    }

    private void preCheck() {
        preCheckInit();
        adjustChannelNumber();

        if (needChannelNumber <= 0) {
            needChannelNumber = 1;
        }
        preCheckReader();
        preCheckWriter();
        LOG.info("PreCheck通过");
    }

    private void preCheckInit() {
        jobId = configuration.getLong(
                CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, -1);

        if (jobId < 0) {
            LOG.info("Set jobId = 0");
            jobId = 0;
            configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID,
                    jobId);
        }

        Thread.currentThread().setName("job-" + jobId);

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(
                getAbstractContainerCommunicator());
        jobReader = preCheckReaderInit(jobPluginCollector);
        jobWriter = preCheckWriterInit(jobPluginCollector);
    }

    private Reader.Job preCheckReaderInit(JobPluginCollector jobPluginCollector) {
        readerPluginName = configuration.getString(
                CoreConstant.DATAX_JOB_CONTENT_READER_NAME);
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, readerPluginName));

        Reader.Job jobReader = (Reader.Job) LoadUtil.loadJobPlugin(
                PluginType.READER, readerPluginName);

        configuration.set(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER + ".dryRun", true);

        // 设置reader的jobConfig
        jobReader.setPluginJobReaderWriterParamConf(configuration.getConfig(
                CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));
        // 设置reader的readerConfig
        jobReader.setPeerPluginJobReaderWriterParamConf(configuration.getConfig(
                CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

        jobReader.jobPluginCollector = jobPluginCollector;

        classLoaderSwapper.restoreCurrentThreadClassLoader();
        return jobReader;
    }


    private Writer.Job preCheckWriterInit(JobPluginCollector jobPluginCollector) {
        writerPluginName = configuration.getString(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, writerPluginName));

        Writer.Job jobWriter = (Writer.Job) LoadUtil.loadJobPlugin(
                PluginType.WRITER, writerPluginName);

        configuration.set(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER + ".dryRun", true);

        // 设置writer的jobConfig
        jobWriter.setPluginJobReaderWriterParamConf(configuration.getConfig(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));
        // 设置reader的readerConfig
        jobWriter.setPeerPluginJobReaderWriterParamConf(configuration.getConfig(
                CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

        jobWriter.setPeerPluginName(readerPluginName);
        jobWriter.jobPluginCollector = jobPluginCollector;

        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return jobWriter;
    }

    private void preCheckReader() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, readerPluginName));
        LOG.info(String.format("DataX Reader.Job [%s] do preCheck work .",
                readerPluginName));
        jobReader.preCheck();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void preCheckWriter() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, writerPluginName));
        LOG.info(String.format("DataX Writer.Job [%s] do preCheck work .",
                writerPluginName));
        jobWriter.preCheck();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    /**
     * reader和writer的初始化
     */
    private void init() {
        jobId = configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, -1);

        if (jobId < 0) {
            LOG.info("Set jobId = 0");
            jobId = 0;
            configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, jobId);
        }

        Thread.currentThread().setName("job-" + jobId);

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(getAbstractContainerCommunicator());

        // 必须先Reader ，后Writer
        jobReader = initJobReader(jobPluginCollector);
        jobWriter = initJobWriter(jobPluginCollector);
    }

    private void prepare() {
        prepareJobReader();
        prepareJobWriter();
    }

    private void preHandle() {
        String handlerPluginTypeStr = configuration.getString(CoreConstant.DATAX_JOB_PREHANDLER_PLUGINTYPE);
        if (!StringUtils.isNotEmpty(handlerPluginTypeStr)) {
            return;
        }

        PluginType handlerPluginType;
        try {
            handlerPluginType = PluginType.valueOf(handlerPluginTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw DataXException.build(FrameworkErrorCode.CONFIG_ERROR,
                    String.format("Job preHandler's pluginType(%s) set error, reason(%s)",
                            handlerPluginTypeStr.toUpperCase(), e.getMessage()));
        }

        String handlerPluginName = configuration.getString(CoreConstant.DATAX_JOB_PREHANDLER_PLUGINNAME);

        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(handlerPluginType, handlerPluginName));

        AbstractJobPlugin handler = LoadUtil.loadJobPlugin(handlerPluginType, handlerPluginName);

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(getAbstractContainerCommunicator());
        handler.jobPluginCollector = jobPluginCollector;

        //todo configuration的安全性，将来必须保证
        handler.preHandler(configuration);
        classLoaderSwapper.restoreCurrentThreadClassLoader();

        LOG.info("After PreHandler: \n" + Engine.filterJobConfiguration(configuration) + "\n");
    }

    private void postHandle() {
        String handlerPluginTypeStr = configuration.getString(CoreConstant.DATAX_JOB_POSTHANDLER_PLUGINTYPE);
        if (StringUtils.isBlank(handlerPluginTypeStr)) {
            return;
        }

        PluginType handlerPluginType;
        try {
            handlerPluginType = PluginType.valueOf(handlerPluginTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw DataXException.build(
                    FrameworkErrorCode.CONFIG_ERROR,
                    String.format("Job postHandler's pluginType(%s) set error, reason(%s)", handlerPluginTypeStr.toUpperCase(), e.getMessage()));
        }

        String handlerPluginName = configuration.getString(CoreConstant.DATAX_JOB_POSTHANDLER_PLUGINNAME);

        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(handlerPluginType, handlerPluginName));

        AbstractJobPlugin handler = LoadUtil.loadJobPlugin(handlerPluginType, handlerPluginName);

        JobPluginCollector jobPluginCollector = new DefaultJobPluginCollector(getAbstractContainerCommunicator());
        handler.jobPluginCollector = jobPluginCollector;

        handler.postHandler(configuration);
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }


    /**
     * 执行reader和writer最细粒度的切分，需要注意的是，writer的切分结果要参照reader的切分结果，
     * 达到切分后数目相等，才能满足1：1的通道模型，所以这里可以将reader和writer的配置整合到一起，
     * 然后，为避免顺序给读写端带来长尾影响，将整合的结果shuffler掉
     */
    private int split() {
        adjustChannelNumber();

        if (needChannelNumber <= 0) {
            needChannelNumber = 1;
        }

        List<Configuration> pluginJobReaderParamConfList = doReaderSplit(needChannelNumber);
        List<Configuration> pluginJobWriterParamConfList = doWriterSplit(pluginJobReaderParamConfList.size());

        List<Configuration> transformerList = configuration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT_TRANSFORMER);

        // 把多对的reader和writer各自组成content的1个的{}元素,这个时候还没有分配到taskGroup
        List<Configuration> contentElementConfigList =
                mergeReaderAndWriterTaskConfigs(pluginJobReaderParamConfList, pluginJobWriterParamConfList, transformerList);

        LOG.debug("contentElementConfigList: {}", JSON.toJSONString(contentElementConfigList));

        configuration.set(CoreConstant.DATAX_JOB_CONTENT, contentElementConfigList);

        return contentElementConfigList.size();
    }

    private void adjustChannelNumber() {
        int needChannelNumberByByte = Integer.MAX_VALUE;

        // 有没有限制总的byte速度
        boolean isByteLimit = (configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_BYTE, 0) > 0);
        if (isByteLimit) {
            long globalLimitedByteSpeed = configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_BYTE, 10 * 1024 * 1024);

            // 当个channel的
            Long channelLimitedByteSpeed = configuration.getLong(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_BYTE);
            if (channelLimitedByteSpeed == null || channelLimitedByteSpeed <= 0) {
                throw DataXException.build(FrameworkErrorCode.CONFIG_ERROR, "在有总bps限速条件下单个channel的bps值不能为空也不能为非正数");
            }

            needChannelNumberByByte = (int) (globalLimitedByteSpeed / channelLimitedByteSpeed);
            needChannelNumberByByte = needChannelNumberByByte > 0 ? needChannelNumberByByte : 1;

            LOG.info("Job set Max-Byte-Speed to " + globalLimitedByteSpeed + " bytes.");
        }

        int needChannelNumberByRecord = Integer.MAX_VALUE;
        // 有没有限制总的record速度
        boolean isRecordLimit = (configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_RECORD, 0)) > 0;
        if (isRecordLimit) {
            // 总的
            long globalLimitedRecordSpeed = configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_RECORD, 100000);

            // 单个channel的
            Long channelLimitedRecordSpeed = configuration.getLong(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_SPEED_RECORD);
            if (channelLimitedRecordSpeed == null || channelLimitedRecordSpeed <= 0) {
                throw DataXException.build(FrameworkErrorCode.CONFIG_ERROR, "在有总tps限速条件下单个channel的tps值不能为空也不能为非正数");
            }

            needChannelNumberByRecord = (int) (globalLimitedRecordSpeed / channelLimitedRecordSpeed);
            needChannelNumberByRecord = needChannelNumberByRecord > 0 ? needChannelNumberByRecord : 1;

            LOG.info("Job set Max-Record-Speed to " + globalLimitedRecordSpeed + " records.");
        }

        // 取较小值
        needChannelNumber = Math.min(needChannelNumberByByte, needChannelNumberByRecord);

        // 如果从byte或record上设置了needChannelNumber
        if (needChannelNumber < Integer.MAX_VALUE) {
            return;
        }

        // 都不能的话只能读取直接设置channel数量
        boolean isChannelLimit = (configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_CHANNEL, 0) > 0);
        if (isChannelLimit) {
            needChannelNumber = configuration.getInt(CoreConstant.DATAX_JOB_SETTING_SPEED_CHANNEL);
            LOG.info("Job set Channel-Number to " + needChannelNumber + " channels.");
            return;
        }

        throw DataXException.build(FrameworkErrorCode.CONFIG_ERROR, "Job运行速度必须设置");
    }

    /**
     * schedule首先完成的工作是把上一步reader和writer split的结果整合到具体taskGroupContainer中,
     * 同时不同的执行模式调用不同的调度策略，将所有任务调度起来
     */
    private void schedule() {
        // 这里的全局speed和每个channel的速度设置为B/s
        int channelsPerTaskGroup = configuration.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, 5);

        // 这个时候的configuration是原始的尚未切的,1个contentElement对应task
        int contentElementConfNumber = configuration.getList(CoreConstant.DATAX_JOB_CONTENT).size();

        needChannelNumber = Math.min(needChannelNumber, contentElementConfNumber);

        PerfTrace.getInstance().setChannelNumber(needChannelNumber);

        // 通过获取配置信息确定各个taskGroup包含哪些task(contentElement)
        // 把content包含的各个元素(切分后的各个小任务)分配到taskGroup
        List<Configuration> taskGroupConfigList =
                JobAssignUtil.assignFairly(configuration, needChannelNumber, channelsPerTaskGroup);

        LOG.info("Scheduler starts [{}] taskGroups.", taskGroupConfigList.size());

        ExecuteMode executeMode = null;
        try {
            executeMode = ExecuteMode.STANDALONE;
            AbstractScheduler abstractScheduler = initStandaloneScheduler(configuration);

            // 设置 executeMode
            for (Configuration taskGroupConfig : taskGroupConfigList) {
                taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, executeMode.getValue());
            }

            if (executeMode == ExecuteMode.LOCAL || executeMode == ExecuteMode.DISTRIBUTE) {
                if (jobId <= 0) {
                    throw DataXException.build(FrameworkErrorCode.RUNTIME_ERROR,
                            "在[ local | distribute ]模式下必须设置jobId，并且其值 > 0 .");
                }
            }

            LOG.info("Running by {} Mode.", executeMode);

            startTransferTimeStamp = System.currentTimeMillis();
            abstractScheduler.schedule(taskGroupConfigList);
            endTransferTimeStamp = System.currentTimeMillis();
        } catch (Exception e) {
            LOG.error("运行scheduler 模式[{}]出错.", executeMode);
            endTransferTimeStamp = System.currentTimeMillis();
            throw DataXException.build(FrameworkErrorCode.RUNTIME_ERROR, e);
        }

        // 检查任务执行情况
        checkLimit();
    }

    private AbstractScheduler initStandaloneScheduler(Configuration configuration) {
        AbstractContainerCommunicator containerCommunicator = new StandAloneJobContainerCommunicator(configuration);
        this.abstractContainerCommunicator = containerCommunicator;

        return new StandAloneScheduler(containerCommunicator);
    }

    private void post() {
        postJobWriter();
        postJobReader();
    }

    private void destroy() {
        if (jobWriter != null) {
            jobWriter.destroy();
            jobWriter = null;
        }
        if (jobReader != null) {
            jobReader.destroy();
            jobReader = null;
        }
    }

    private void logStatistics() {
        long totalCosts = (endTimeStamp - startTimeStamp) / 1000;
        long transferCosts = (endTransferTimeStamp - startTransferTimeStamp) / 1000;
        if (0L == transferCosts) {
            transferCosts = 1L;
        }

        if (super.getAbstractContainerCommunicator() == null) {
            return;
        }

        Communication communication = super.getAbstractContainerCommunicator().collect();
        communication.setTimestamp(endTimeStamp);

        Communication tempComm = new Communication();
        tempComm.setTimestamp(startTransferTimeStamp);

        Communication reportCommunication = CommunicationTool.getReportComm(communication, tempComm, splitCount);

        // 字节速率
        long byteSpeedPerSecond = communication.getLongCounter(CommunicationTool.READ_SUCCEED_BYTES) / transferCosts;
        long recordSpeedPerSecond = communication.getLongCounter(CommunicationTool.READ_SUCCEED_RECORDS) / transferCosts;

        reportCommunication.setLongCounter(CommunicationTool.BYTE_SPEED, byteSpeedPerSecond);
        reportCommunication.setLongCounter(CommunicationTool.RECORD_SPEED, recordSpeedPerSecond);

        getAbstractContainerCommunicator().report(reportCommunication);


        LOG.info(String.format(
                "\n" + "%-26s: %-18s\n" + "%-26s: %-18s\n" + "%-26s: %19s\n"
                        + "%-26s: %19s\n" + "%-26s: %19s\n" + "%-26s: %19s\n"
                        + "%-26s: %19s\n",
                "任务启动时刻", dateFormat.format(startTimeStamp),
                "任务结束时刻", dateFormat.format(endTimeStamp),
                "任务总计耗时", totalCosts + "s",
                "任务平均流量", StrUtil.stringify(byteSpeedPerSecond) + "/s",
                "记录写入速度", recordSpeedPerSecond + "rec/s",
                "读出记录总数", CommunicationTool.getTotalReadRecordCount(communication),
                "读写失败总数", CommunicationTool.getTotalErrorRecords(communication)
        ));

        if (communication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS) > 0
                || communication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS) > 0
                || communication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS) > 0) {
            LOG.info(String.format(
                    "\n" + "%-26s: %19s\n" + "%-26s: %19s\n" + "%-26s: %19s\n",
                    "Transformer成功记录总数",
                    communication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS),

                    "Transformer失败记录总数",
                    communication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS),

                    "Transformer过滤记录总数",
                    communication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS)
            ));
        }


    }

    /**
     * reader job的初始化，返回Reader.Job
     */
    private Reader.Job initJobReader(JobPluginCollector jobPluginCollector) {
        readerPluginName = configuration.getString(CoreConstant.DATAX_JOB_CONTENT_READER_NAME);

        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, readerPluginName));

        Reader.Job jobReader = (Reader.Job) LoadUtil.loadJobPlugin(PluginType.READER, readerPluginName);

        // 设置reader的jobConfig
        jobReader.setPluginJobReaderWriterParamConf(configuration.getConfig(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));
        // 设置reader的readerConfig
        jobReader.setPeerPluginJobReaderWriterParamConf(configuration.getConfig(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));
        jobReader.jobPluginCollector = jobPluginCollector;

        jobReader.init();

        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return jobReader;
    }

    /**
     * writer job的初始化，返回Writer.Job
     */
    private Writer.Job initJobWriter(JobPluginCollector jobPluginCollector) {
        writerPluginName = configuration.getString(CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);

        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, writerPluginName));

        Writer.Job jobWriter = (Writer.Job) LoadUtil.loadJobPlugin(PluginType.WRITER, writerPluginName);

        // 设置writer的jobConfig
        jobWriter.setPluginJobReaderWriterParamConf(configuration.getConfig(CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER));
        // 设置reader的readerConfig
        jobWriter.setPeerPluginJobReaderWriterParamConf(configuration.getConfig(CoreConstant.DATAX_JOB_CONTENT_READER_PARAMETER));

        jobWriter.setPeerPluginName(readerPluginName);
        jobWriter.jobPluginCollector = jobPluginCollector;

        jobWriter.init();

        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return jobWriter;
    }

    private void prepareJobReader() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.READER, readerPluginName));
        LOG.info(String.format("DataX Reader.Job [%s] do prepare work .",
                readerPluginName));
        jobReader.prepare();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void prepareJobWriter() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, writerPluginName));
        LOG.info(String.format("DataX Writer.Job [%s] do prepare work .",
                writerPluginName));
        jobWriter.prepare();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    // TODO: 如果源头就是空数据
    private List<Configuration> doReaderSplit(int adviceNumber) {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, readerPluginName));

        List<Configuration> pluginJobReaderWriterParamConfList = jobReader.split(adviceNumber);
        if (CollectionUtils.isEmpty(pluginJobReaderWriterParamConfList)) {
            throw DataXException.build(FrameworkErrorCode.PLUGIN_SPLIT_ERROR, "reader切分的task数目不能小于等于0");
        }
        LOG.info("DataX Reader.Job [{}] splits to [{}] tasks.", readerPluginName, pluginJobReaderWriterParamConfList.size());

        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return pluginJobReaderWriterParamConfList;
    }

    private List<Configuration> doWriterSplit(int readerTaskNumber) {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.WRITER, writerPluginName));

        List<Configuration> writerSlicesConfigs = jobWriter.split(readerTaskNumber);
        if (CollectionUtils.isEmpty(writerSlicesConfigs)) {
            throw DataXException.build(FrameworkErrorCode.PLUGIN_SPLIT_ERROR, "writer切分的task不能小于等于0");
        }
        LOG.info("DataX Writer.Job [{}] splits to [{}] tasks.", writerPluginName, writerSlicesConfigs.size());

        classLoaderSwapper.restoreCurrentThreadClassLoader();

        return writerSlicesConfigs;
    }

    /**
     * 按顺序整合reader和writer的配置，这里的顺序不能乱！ 输入是reader、writer级别的配置，输出是一个完整task的配置
     */
    private List<Configuration> mergeReaderAndWriterTaskConfigs(
            List<Configuration> readerTasksConfigs,
            List<Configuration> writerTasksConfigs) {
        return mergeReaderAndWriterTaskConfigs(readerTasksConfigs, writerTasksConfigs, null);
    }

    private List<Configuration> mergeReaderAndWriterTaskConfigs(List<Configuration> pluginJobReaderParamConfList,
                                                                List<Configuration> pluginJobWriterParamConfList,
                                                                List<Configuration> transformerConfigs) {
        if (pluginJobReaderParamConfList.size() != pluginJobWriterParamConfList.size()) {
            throw DataXException.build(
                    FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                    String.format("reader切分的task数目[%d]不等于writer切分的task数目[%d].",
                            pluginJobReaderParamConfList.size(), pluginJobWriterParamConfList.size())
            );
        }

        List<Configuration> contentElementConfList = new ArrayList<>();

        for (int i = 0; i < pluginJobReaderParamConfList.size(); i++) {
            Configuration contentElementConf = Configuration.newDefault();

            contentElementConf.set(CoreConstant.JOB_READER_NAME, readerPluginName);
            contentElementConf.set(CoreConstant.JOB_READER_PARAMETER, pluginJobReaderParamConfList.get(i));

            contentElementConf.set(CoreConstant.JOB_WRITER_NAME, writerPluginName);
            contentElementConf.set(CoreConstant.JOB_WRITER_PARAMETER, pluginJobWriterParamConfList.get(i));

            if (transformerConfigs != null && transformerConfigs.size() > 0) {
                contentElementConf.set(CoreConstant.JOB_TRANSFORMER, transformerConfigs);
            }

            contentElementConf.set(CoreConstant.TASK_ID, i);
            contentElementConfList.add(contentElementConf);
        }

        return contentElementConfList;
    }

    /**
     * 这里比较复杂，分两步整合 1. tasks到channel 2. channel到taskGroup
     * 合起来考虑，其实就是把tasks整合到taskGroup中，需要满足计算出的channel数，同时不能多起channel
     * <p/>
     * example:
     * <p/>
     * 前提条件： 切分后是1024个分表，假设用户要求总速率是1000M/s，每个channel的速率的3M/s，
     * 每个taskGroup负责运行7个channel
     * <p/>
     * 计算： 总channel数为：1000M/s / 3M/s =
     * 333个，为平均分配，计算可知有308个每个channel有3个tasks，而有25个每个channel有4个tasks，
     * 需要的taskGroup数为：333 / 7 =
     * 47...4，也就是需要48个taskGroup，47个是每个负责7个channel，有4个负责1个channel
     * <p/>
     * 处理：我们先将这负责4个channel的taskGroup处理掉，逻辑是：
     * 先按平均为3个tasks找4个channel，设置taskGroupId为0，
     * 接下来就像发牌一样轮询分配task到剩下的包含平均channel数的taskGroup中
     * <p/>
     * TODO delete it
     *
     * @return 每个taskGroup独立的全部配置
     */
    @SuppressWarnings("serial")
    private List<Configuration> distributeTasksToTaskGroup(int averTaskPerChannel,
                                                           int channelNumber,
                                                           int channelsPerTaskGroup) {
        Validate.isTrue(averTaskPerChannel > 0 && channelNumber > 0
                        && channelsPerTaskGroup > 0,
                "每个channel的平均task数[averTaskPerChannel]，channel数目[channelNumber]，每个taskGroup的平均channel数[channelsPerTaskGroup]都应该为正数");
        List<Configuration> taskConfigs = configuration
                .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
        int taskGroupNumber = channelNumber / channelsPerTaskGroup;
        int leftChannelNumber = channelNumber % channelsPerTaskGroup;
        if (leftChannelNumber > 0) {
            taskGroupNumber += 1;
        }

        // 如果只有一个taskGroup，直接打标返回
        if (taskGroupNumber == 1) {
            final Configuration taskGroupConfig = configuration.clone();

            // configure的clone不能clone出
            taskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT, configuration
                    .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT));
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL,
                    channelNumber);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, 0);
            return new ArrayList<Configuration>() {
                {
                    add(taskGroupConfig);
                }
            };
        }

        List<Configuration> taskGroupConfigs = new ArrayList<Configuration>();

        // 将每个taskGroup中content的配置清空
        for (int i = 0; i < taskGroupNumber; i++) {
            Configuration taskGroupConfig = configuration.clone();
            List<Configuration> taskGroupJobContent = taskGroupConfig
                    .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
            taskGroupJobContent.clear();
            taskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT, taskGroupJobContent);

            taskGroupConfigs.add(taskGroupConfig);
        }

        int taskConfigIndex = 0;
        int channelIndex = 0;
        int taskGroupConfigIndex = 0;

        // 先处理掉taskGroup包含channel数不是平均值的taskGroup
        if (leftChannelNumber > 0) {
            Configuration taskGroupConfig = taskGroupConfigs.get(taskGroupConfigIndex);
            for (; channelIndex < leftChannelNumber; channelIndex++) {
                for (int i = 0; i < averTaskPerChannel; i++) {
                    List<Configuration> taskGroupJobContent = taskGroupConfig
                            .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
                    taskGroupJobContent.add(taskConfigs.get(taskConfigIndex++));
                    taskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT,
                            taskGroupJobContent);
                }
            }

            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL,
                    leftChannelNumber);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID,
                    taskGroupConfigIndex++);
        }

        // 下面需要轮询分配，并打上channel数和taskGroupId标记
        int equalDivisionStartIndex = taskGroupConfigIndex;
        while (taskConfigIndex < taskConfigs.size()
                && equalDivisionStartIndex < taskGroupConfigs.size()) {
            for (taskGroupConfigIndex = equalDivisionStartIndex; taskGroupConfigIndex < taskGroupConfigs
                    .size() && taskConfigIndex < taskConfigs.size(); taskGroupConfigIndex++) {
                Configuration taskGroupConfig = taskGroupConfigs.get(taskGroupConfigIndex);
                List<Configuration> taskGroupJobContent = taskGroupConfig
                        .getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
                taskGroupJobContent.add(taskConfigs.get(taskConfigIndex++));
                taskGroupConfig.set(
                        CoreConstant.DATAX_JOB_CONTENT, taskGroupJobContent);
            }
        }

        for (taskGroupConfigIndex = equalDivisionStartIndex;
             taskGroupConfigIndex < taskGroupConfigs.size(); ) {
            Configuration taskGroupConfig = taskGroupConfigs.get(taskGroupConfigIndex);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL,
                    channelsPerTaskGroup);
            taskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID,
                    taskGroupConfigIndex++);
        }

        return taskGroupConfigs;
    }

    private void postJobReader() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(PluginType.READER, readerPluginName));
        LOG.info("DataX Reader.Job [{}] do post work.", readerPluginName);
        jobReader.post();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    private void postJobWriter() {
        classLoaderSwapper.setCurrentThreadClassLoader(LoadUtil.getJarLoader(
                PluginType.WRITER, writerPluginName));
        LOG.info("DataX Writer.Job [{}] do post work.",
                writerPluginName);
        jobWriter.post();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }

    /**
     * 检查最终结果是否超出阈值，如果阈值设定小于1，则表示百分数阈值，大于1表示条数阈值。
     *
     * @param
     */
    private void checkLimit() {
        Communication communication = super.getAbstractContainerCommunicator().collect();
        errorLimit.checkRecordLimit(communication);
        errorLimit.checkPercentageLimit(communication);
    }

    /**
     * 调用外部hook
     */
    private void invokeHooks() {
        Communication comm = super.getAbstractContainerCommunicator().collect();
        HookInvoker invoker = new HookInvoker(CoreConstant.DATAX_HOME + "/hook", configuration, comm.getKey_number());
        invoker.invokeAll();
    }
}
