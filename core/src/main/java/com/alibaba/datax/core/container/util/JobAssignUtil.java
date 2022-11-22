package com.alibaba.datax.core.container.util;

import com.alibaba.datax.common.constant.CommonConstant;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public final class JobAssignUtil {
    private JobAssignUtil() {
    }

    /**
     * 公平的分配 task 到对应的 taskGroup 中。
     * 公平体现在：会考虑 task 中对资源负载作的 load 标识进行更均衡的作业分配操作。
     * TODO 具体文档举例说明
     */
    public static List<Configuration> assignFairly(Configuration configuration,
                                                   int channelNumber,
                                                   int channelCountPerTaskGroup) {

        Validate.isTrue(configuration != null, "框架获得的 Job 不能为 null.");

        List<Configuration> contentConfigList = configuration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);
        Validate.isTrue(contentConfigList.size() > 0, "框架获得的切分后的 Job 无内容.");

        Validate.isTrue(channelNumber > 0 && channelCountPerTaskGroup > 0,
                "每个channel的平均task数[averTaskPerChannel]，channel数目[channelNumber]，每个taskGroup的平均channel数[channelsPerTaskGroup]都应该为正数");

        int taskGroupNumber = (int) Math.ceil(1.0 * channelNumber / channelCountPerTaskGroup);

        Configuration contentConfig = contentConfigList.get(0);

        // reader.parameter.loadBalanceResourceMark
        String readerResourceMark = contentConfig.getString(CoreConstant.JOB_READER_PARAMETER + "." + CommonConstant.LOAD_BALANCE_RESOURCE_MARK);
        // writer.parameter.loadBalanceResourceMark
        String writerResourceMark = contentConfig.getString(CoreConstant.JOB_WRITER_PARAMETER + "." + CommonConstant.LOAD_BALANCE_RESOURCE_MARK);

        boolean hasLoadBalanceResourceMark =
                StringUtils.isNotBlank(readerResourceMark) || StringUtils.isNotBlank(writerResourceMark);

        // fake 1个固定的 key 作为资源标识（在 reader 或者 writer 上均可，此处选择在 reader 上进行 fake）
        if (!hasLoadBalanceResourceMark) {
            for (Configuration conf : contentConfigList) {
                conf.set(CoreConstant.JOB_READER_PARAMETER + "." + CommonConstant.LOAD_BALANCE_RESOURCE_MARK,
                        "aFakeResourceMarkForLoadBalance");
            }

            // 是为了避免某些插件没有设置 资源标识 而进行了一次随机打乱操作
            Collections.shuffle(contentConfigList, new Random(System.currentTimeMillis()));
        }

        LinkedHashMap<String, List<Integer>> resMark_TaskIdList = parseAndGetResMarkAndTaskIdMap(contentConfigList);

        // split得到的对个content的{}元素再分到多个task group
        List<Configuration> taskGroupConfigList = doAssign(resMark_TaskIdList, configuration, taskGroupNumber);

        // 调整 每个 taskGroup 对应的 Channel 个数（属于优化范畴）
        adjustChannelNumPerTaskGroup(taskGroupConfigList, channelNumber);

        return taskGroupConfigList;
    }

    private static void adjustChannelNumPerTaskGroup(List<Configuration> taskGroupConfigList, int channelNumber) {
        int taskGroupNumber = taskGroupConfigList.size();
        int avgChannelsPerTaskGroup = channelNumber / taskGroupNumber;
        int remainderChannelCount = channelNumber % taskGroupNumber;
        // 表示有 remainderChannelCount 个 taskGroup,其对应 Channel 个数应该为：avgChannelsPerTaskGroup + 1；
        // （taskGroupNumber - remainderChannelCount）个 taskGroup,其对应 Channel 个数应该为：avgChannelsPerTaskGroup

        int i = 0;
        for (; i < remainderChannelCount; i++) {
            taskGroupConfigList.get(i).set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, avgChannelsPerTaskGroup + 1);
        }

        for (int j = 0; j < taskGroupNumber - remainderChannelCount; j++) {
            taskGroupConfigList.get(i + j).set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL, avgChannelsPerTaskGroup);
        }
    }

    /**
     * 根据task 配置，获取到：
     * 资源名称 --> taskId(List) 的 map 映射关系
     */
    private static LinkedHashMap<String, List<Integer>> parseAndGetResMarkAndTaskIdMap(List<Configuration> contentConfigList) {
        // key: resourceMark, value: taskId
        LinkedHashMap<String, List<Integer>> readerResMark_taskIdList = new LinkedHashMap<>();
        LinkedHashMap<String, List<Integer>> writerResMarkA_taskIdList = new LinkedHashMap<>();

        for (Configuration contentConfig : contentConfigList) {
            int taskId = contentConfig.getInt(CoreConstant.TASK_ID);

            // 把 readerResourceMark 加到 readerResourceMarkAndTaskIdMap 中
            String readerResourceMark = contentConfig.getString(CoreConstant.JOB_READER_PARAMETER + "." + CommonConstant.LOAD_BALANCE_RESOURCE_MARK);
            readerResMark_taskIdList.computeIfAbsent(readerResourceMark, k -> new LinkedList<>());
            readerResMark_taskIdList.get(readerResourceMark).add(taskId);

            // 把 writerResourceMark 加到 writerResourceMarkAndTaskIdMap 中
            String writerResourceMark = contentConfig.getString(CoreConstant.JOB_WRITER_PARAMETER + "." + CommonConstant.LOAD_BALANCE_RESOURCE_MARK);
            writerResMarkA_taskIdList.computeIfAbsent(writerResourceMark, k -> new LinkedList<>());
            writerResMarkA_taskIdList.get(writerResourceMark).add(taskId);
        }

        if (readerResMark_taskIdList.size() >= writerResMarkA_taskIdList.size()) {
            // 用 reader 对资源做的标记进行 shuffle
            return readerResMark_taskIdList;
        }

        // 用 writer 对资源做的标记进行 shuffle
        return writerResMarkA_taskIdList;

    }


    /**
     * /**
     * 需要实现的效果通过例子来说是：
     * <pre>
     * a 库上有表：0, 1, 2
     * a 库上有表：3, 4
     * c 库上有表：5, 6, 7
     *
     * 如果有 4个 taskGroup
     * 则 assign 后的结果为：
     * taskGroup-0: 0,  4,
     * taskGroup-1: 3,  6,
     * taskGroup-2: 5,  2,
     * taskGroup-3: 1,  7
     *
     * </pre>
     */
    private static List<Configuration> doAssign(LinkedHashMap<String, List<Integer>> resMark_TaskIdList,
                                                Configuration jobConfiguration,
                                                int taskGroupNumber) {
        List<Configuration> contentConfigList = jobConfiguration.getListConfiguration(CoreConstant.DATAX_JOB_CONTENT);

        Configuration taskGroupTemplate = jobConfiguration.clone();
        taskGroupTemplate.remove(CoreConstant.DATAX_JOB_CONTENT);

        List<Configuration> result = new LinkedList<>();

        List<List<Configuration>> contentConfigList1 = new ArrayList<>(taskGroupNumber);
        for (int i = 0; i < taskGroupNumber; i++) {
            contentConfigList1.add(new LinkedList<>());
        }

        int mapValueMaxLength = -1;

        List<String> resMarkList = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry : resMark_TaskIdList.entrySet()) {
            resMarkList.add(entry.getKey());

            if (entry.getValue().size() > mapValueMaxLength) {
                mapValueMaxLength = entry.getValue().size();
            }
        }

        int taskGroupIndex = 0;
        for (int i = 0; i < mapValueMaxLength; i++) {
            for (String resMark : resMarkList) {
                if (resMark_TaskIdList.get(resMark).size() == 0) {
                    continue;
                }

                int taskId = resMark_TaskIdList.get(resMark).get(0);
                contentConfigList1.get(taskGroupIndex % taskGroupNumber).add(contentConfigList.get(taskId));
                taskGroupIndex++;

                resMark_TaskIdList.get(resMark).remove(0);
            }
        }

        for (int i = 0; i < taskGroupNumber; i++) {
            Configuration tempTaskGroupConfig = taskGroupTemplate.clone();
            tempTaskGroupConfig.set(CoreConstant.DATAX_JOB_CONTENT, contentConfigList1.get(i));
            tempTaskGroupConfig.set(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID, i);

            result.add(tempTaskGroupConfig);
        }

        return result;
    }

}
