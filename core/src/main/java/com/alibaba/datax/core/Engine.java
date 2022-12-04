package com.alibaba.datax.core;

import com.alibaba.datax.common.distribute.DispatcherInfo;
import com.alibaba.datax.common.element.ColumnCast;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.constant.Constant;
import com.alibaba.datax.common.util.ExecuteMode;
import com.alibaba.datax.common.util.MessageSource;
import com.alibaba.datax.core.container.AbstractContainer;
import com.alibaba.datax.core.job.JobContainer;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.util.*;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.alibaba.fastjson.JSON;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Engine是DataX入口类，该类负责初始化Job或者Task的运行容器，并运行插件的Job或者Task逻辑
 */
public class Engine {
    private static final Logger LOG = LoggerFactory.getLogger(Engine.class);

    /* check job model (job/task) first */
    private void start(Configuration totalConfig) {

        // 绑定column转换信息
        ColumnCast.bind(totalConfig);

        // 初始化PluginLoader，可以获取各种插件配置
        LoadUtil.bind(totalConfig);

        // boolean isJob = !("taskGroup".equalsIgnoreCase(totalConfig.getString(CoreConstant.DATAX_CORE_CONTAINER_MODEL)));
        boolean isJob = !ExecuteMode.taskGroup.equals(Global.mode);
        totalConfig.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, Global.mode.name());

        // jobContainer会在schedule后再行进行设置和调整值
        int channelNumber = 0;

        AbstractContainer abstractContainer;
        int taskGroupId = -1;
        if (isJob) {
            abstractContainer = new JobContainer(totalConfig);
        } else { // 应该是分布式版本中的其他节点收到的任务的片段(taskGroup)
            abstractContainer = new TaskGroupContainer(totalConfig);
            taskGroupId = totalConfig.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            channelNumber = totalConfig.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL);
        }

        // 缺省打开perfTrace
        boolean traceEnable = totalConfig.getBool(CoreConstant.DATAX_CORE_CONTAINER_TRACE_ENABLE, true);
        boolean perfReportEnable = totalConfig.getBool(CoreConstant.DATAX_CORE_REPORT_DATAX_PERFLOG, true);

        // standalone模式的 datax shell任务不进行汇报
        /*if (instanceId == -1) {
            perfReportEnable = false;
        }*/

        int priority = 0;
        try {
            priority = Integer.parseInt(System.getenv("SKYNET_PRIORITY"));
        } catch (NumberFormatException e) {
            LOG.warn("prioriy set to 0, because NumberFormatException, the value is: " + System.getProperty("PROIORY"));
        }

        Configuration jobInfoConfig = totalConfig.getConfig(CoreConstant.DATAX_JOB_JOBINFO);

        // 初始化PerfTrace
        PerfTrace perfTrace = PerfTrace.getInstance(isJob, Global.jobId, taskGroupId, priority, traceEnable);
        perfTrace.setJobInfo(jobInfoConfig, perfReportEnable, channelNumber);

        abstractContainer.start();
    }


    // 注意屏蔽敏感信息
    public static String filterJobConfiguration(final Configuration configuration) {
        Configuration jobConfWithSetting = configuration.getConfig("job").clone();

        Configuration jobContent = jobConfWithSetting.getConfig("content");

        filterSensitiveConfiguration(jobContent);

        jobConfWithSetting.set("content", jobContent);

        return jobConfWithSetting.beautify();
    }

    public static void filterSensitiveConfiguration(Configuration configuration) {
        Set<String> keys = configuration.getKeys();
        for (final String key : keys) {
            boolean isSensitive = StringUtils.endsWithIgnoreCase(key, "password")
                    || StringUtils.endsWithIgnoreCase(key, "accessKey");
            if (isSensitive && configuration.get(key) instanceof String) {
                configuration.set(key, configuration.getString(key).replaceAll(".", "*"));
            }
        }
    }

    public static void entry(String[] args) throws Throwable {
        Options options = new Options();
        options.addOption(Constant.COMMAND_PARAM.job, true, Constant.EMPTY_STRING);
        options.addOption(Constant.COMMAND_PARAM.jobid, true, Constant.EMPTY_STRING);
        options.addOption(Constant.COMMAND_PARAM.mode, true, Constant.EMPTY_STRING);
        // options.addOption(Constant.COMMAND_PARAM.nodeList, true, Constant.EMPTY_STRING);

        CommandLine commandLine = new BasicParser().parse(options, args);

        // 如果用户没有明确指定jobid, 则 datax.py 会指定 jobid 默认值为-1
        Global.jobId = Long.parseLong(commandLine.getOptionValue(Constant.COMMAND_PARAM.jobid));
        if (Global.jobId == Constant.INVALID_ID) {
            throw DataXException.build(FrameworkErrorCode.CONFIG_ERROR, "必须提供有效的jobId");
        }

        Global.mode = ExecuteMode.valueOf(commandLine.getOptionValue(Constant.COMMAND_PARAM.mode));
        if (ExecuteMode.distribute.equals(Global.mode)) {
            NettyServer.run();
        }

        // distribute模式的时候 node_map_json要有
        if (ExecuteMode.distribute.equals(Global.mode)) {
            String nodeListJson = System.getenv(Constant.ENV_PARAM.nodeList);

            if (StringUtils.isBlank(nodeListJson)) {
                throw DataXException.build("distribute模式的时候需要提供node_list");
            }

            Global.nodeList = JSON.parseArray(nodeListJson, DispatcherInfo.class);
        }

        Configuration configuration = ConfigParser.parse(commandLine.getOptionValue(Constant.COMMAND_PARAM.job));

        configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, Global.jobId);

        // i18n
        MessageSource.init(configuration);
        MessageSource.reloadResourceBundle(Configuration.class);

       /* long jobId;
        if (!"-1".equalsIgnoreCase(jobIdString)) {
            jobId = Long.parseLong(jobIdString);
        } else {
            // only for dsc & ds & datax 3 update
            String dscJobUrlPatternString = "/instance/(\\d{1,})/config.xml";
            String dsJobUrlPatternString = "/inner/job/(\\d{1,})/config";
            String dsTaskGroupUrlPatternString = "/inner/job/(\\d{1,})/taskGroup/";
            List<String> patternStringList = Arrays.asList(dscJobUrlPatternString, dsJobUrlPatternString, dsTaskGroupUrlPatternString);
            jobId = parseJobIdFromUrl(patternStringList, jobPath);
        }*/

        // 打印vmInfo
        VMInfo vmInfo = VMInfo.getVmInfo();
        if (vmInfo != null) {
            LOG.info(vmInfo.toString());
        }

        // LOG.info("\n" + Engine.filterJobConfiguration(configuration) + "\n");
        LOG.info(configuration.beautify());

        ConfigurationValidate.doValidate(configuration);

        new Engine().start(configuration);
    }


    /**
     * -1 表示未能解析到 jobId
     * <p>
     * only for dsc & ds & datax 3 update
     */
    private static long parseJobIdFromUrl(List<String> patternStringList, String url) {
        long result = -1;
        for (String patternString : patternStringList) {
            result = doParseJobIdFromUrl(patternString, url);
            if (result != -1) {
                return result;
            }
        }
        return result;
    }

    private static long doParseJobIdFromUrl(String patternString, String url) {
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(url);
        if (matcher.find()) {
            return Long.parseLong(matcher.group(1));
        }

        return -1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = 0;
        try {
            Engine.entry(args);
        } catch (Throwable e) {
            exitCode = 1;
            LOG.error("该任务最可能的错误原因是:", e);

            if (e instanceof DataXException) {
                DataXException tempException = (DataXException) e;
                ErrorCode errorCode = tempException.getErrorCode();
                if (errorCode instanceof FrameworkErrorCode) {
                    FrameworkErrorCode tempErrorCode = (FrameworkErrorCode) errorCode;
                    exitCode = tempErrorCode.toExitValue();
                }
            }

            System.exit(exitCode);
        }

        System.exit(exitCode);
    }
}
