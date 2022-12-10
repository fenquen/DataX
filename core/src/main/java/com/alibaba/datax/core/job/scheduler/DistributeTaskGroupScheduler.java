package com.alibaba.datax.core.job.scheduler;

import com.alibaba.datax.common.distribute.DispatcherInfo;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.constant.Constant;
import com.alibaba.datax.common.util.ExecuteMode;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.statistics.communicator.JobContainerCommunicator;
import com.alibaba.datax.core.util.Global;
import com.alibaba.datax.core.util.HttpUtil;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.fastjson.JSON;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class DistributeTaskGroupScheduler extends AbstractTaskGroupScheduler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributeTaskGroupScheduler.class);

    private Map<DispatcherInfo, List<Configuration>> realNode_taskGroupConfList;

    public DistributeTaskGroupScheduler(JobContainerCommunicator abstractContainerCommunicator) {
        super(abstractContainerCommunicator);
    }

    @Override
    protected synchronized void startAllTaskGroup(List<Configuration> taskGroupConfList) {
        // 发送请求到各个的节点 不过需要首先把自己去掉
        // 如何把多个的taskConfigConf分布到了多个node上 node->taskGroupList
        Map<DispatcherInfo, List<Configuration>> node_taskGroupConfList = dispatchTaskGroupConfList2NodeList(taskGroupConfList, Global.nodeList);
        List<Configuration> localTaskGroupConfList = node_taskGroupConfList.get(Global.masterNode);

        realNode_taskGroupConfList = new HashMap<>(node_taskGroupConfList);

        for (Map.Entry<DispatcherInfo, List<Configuration>> entry : node_taskGroupConfList.entrySet()) {
            DispatcherInfo node = entry.getKey();
            List<Configuration> list = entry.getValue();

            // 应该优先调度远端的node 因为要是失败了使用本地的node调度
            if (Global.masterNode.equals(node)) {
                continue;
            }

            // 因为当前的缺陷 远端的node不支持收取多个taskGroupConf
            if (1 < list.size()) {
                throw DataXException.build("调度到远端的node的taskGroup当前的实现下只能有1个");
            }

            Configuration taskGroupConf = list.get(0);
            List<NameValuePair> nameValuePairList = new ArrayList<>();

            nameValuePairList.add(new BasicNameValuePair(Constant.COMMAND_PARAM.jobid, Global.jobId.toString()));
            // 使用internal的原理是clone()函数
            nameValuePairList.add(new BasicNameValuePair("json", JSON.toJSONString(taskGroupConf.getInternal())));
            nameValuePairList.add(new BasicNameValuePair(Constant.COMMAND_PARAM.mode, ExecuteMode.taskGroup.name()));
            nameValuePairList.add(new BasicNameValuePair(Constant.ENV_PARAM.masterNodeHost, Global.masterNodeHost));
            nameValuePairList.add(new BasicNameValuePair(Constant.ENV_PARAM.masterNodePort, Global.masterNodePort));
            nameValuePairList.add(new BasicNameValuePair(Constant.ENV_PARAM.masterNodeNettyHttpServerPort, Global.masterNodeNettyHttpServerPort));

            String target = HttpUtil.appendUrl(node.host, node.port) + Constant.SPRING_HTTP.START_HTTP_PATH;

            boolean dispatchSuccess = true;
            try {
                HttpUtil.post(target, nameValuePairList);
            } catch (Exception e) {
                realNode_taskGroupConfList.remove(node);

                LOGGER.error(e.getMessage(), e);
                dispatchSuccess = false;
            }

            // 要是调度失败还是只能当前的node来处理
            if (!dispatchSuccess) {
                localTaskGroupConfList.add(taskGroupConf);
            }
        }

        // 本地调度
        scheduleLocally(localTaskGroupConfList);
    }

    /**
     * 要求远端的dispatcher要有相应的pid记录
     */
    @Override
    public synchronized void cancelSchedule(List<Configuration> taskGroupConfigList) {
        // 说明startAllTaskGroup还没有调用过
        if (realNode_taskGroupConfList == null) {
            return;
        }

        for (Map.Entry<DispatcherInfo, List<Configuration>> entry : realNode_taskGroupConfList.entrySet()) {
            DispatcherInfo node = entry.getKey();

            if (Global.masterNode.equals(node)) {
                continue;
            }

            // 要是node对应的taskGroup已完成了那么不用发送命令
            boolean send = false;

            for (Configuration taskGroupConf : entry.getValue()) {
                int taskGroupId = taskGroupConf.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
                Communication taskGroupCommunication = LocalTGCommunicationManager.get(taskGroupId);
                if (taskGroupCommunication.isFinished()) {
                    continue;
                }
                send = true;
                break;
            }

            if (!send) {
                continue;
            }

            List<NameValuePair> nameValuePairList = new ArrayList<>();
            nameValuePairList.add(new BasicNameValuePair(Constant.COMMAND_PARAM.jobid, Global.jobId.toString()));

            String target = HttpUtil.appendUrl(node.host, node.port) + Constant.SPRING_HTTP.STOP_HTTP_PATH;

            try {
                HttpUtil.post(target, nameValuePairList);
            } catch (Exception e) {
                LOGGER.error("cancelSchedule 异常", e);
            }
        }
    }

    private Map<DispatcherInfo, List<Configuration>> dispatchTaskGroupConfList2NodeList(List<Configuration> taskGroupConfList, List<DispatcherInfo> nodeList) {
        // 保留新鲜的和本node
        long now = System.currentTimeMillis();
        nodeList = nodeList.stream().filter(node -> node.equals(Global.masterNode) || now - node.reportTime < 60000).collect(Collectors.toList());

        Map<DispatcherInfo, List<Configuration>> map = new HashMap<>();

        int taskGroupConfCount = taskGroupConfList.size();
        int nodeCount = nodeList.size();

        int min = Math.min(taskGroupConfCount, nodeCount);

        for (int a = 0; a < min; a++) {
            List<Configuration> list = new ArrayList<>();
            list.add(taskGroupConfList.get(a));
            map.put(nodeList.get(a), list);
        }

        // 任务要均匀分摊到各个node node数量的不够要把多的任务倾斜到当前的主node
        // 目前缺陷 远端的node当前还不支持多个taskGroupConf
        if (nodeCount < taskGroupConfCount) {
            List<Configuration> targetList = map.get(Global.masterNode);

            for (int a = min; a < taskGroupConfCount; a++) {
                targetList.add(taskGroupConfList.get(a));
            }
        }

        return map;
    }
}
