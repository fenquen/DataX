package com.alibaba.datax.core.job.scheduler;

import com.alibaba.datax.common.distribute.DispatcherInfo;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.constant.Constant;
import com.alibaba.datax.common.util.ExecuteMode;
import com.alibaba.datax.core.statistics.communicator.AbstractContainerCommunicator;
import com.alibaba.datax.core.statistics.communicator.JobContainerCommunicator;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.Global;
import com.alibaba.datax.core.util.HttpUtil;
import com.alibaba.fastjson.JSON;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;

import java.util.*;

public class DistributeTaskGroupScheduler extends AbstractTaskGroupScheduler {
    public DistributeTaskGroupScheduler(JobContainerCommunicator abstractContainerCommunicator) {
        super(abstractContainerCommunicator);
    }

    @Override
    protected void startAllTaskGroup(List<Configuration> taskGroupConfList) {
        // 发送请求到各个的节点 不过需要首先把自己去掉
        // 如何把多个的taskConfigConf分布到了多个node上 node->taskGroupList

        Map<DispatcherInfo, List<Configuration>> node_taskGroupConfList = dispatchTaskGroupConfList2NodeList(taskGroupConfList, Global.nodeList);

        for (Map.Entry<DispatcherInfo, List<Configuration>> entry : node_taskGroupConfList.entrySet()) {

            DispatcherInfo node = entry.getKey();
            List<Configuration> list = entry.getValue();

            // 调度到本地node上还是和以前相同的套路
            if (Global.localNode.equals(node)) {
                scheduleLocally(taskGroupConfList);
            } else {
                // 因为当前的缺陷 远端的node不支持收取多个taskGroupConf
                if (1 < list.size()) {
                    throw DataXException.build("调度到远端的node的taskGroup当前的实现下只能有1个");
                }

                Configuration taskGroupConf = list.get(0);
                List<NameValuePair> nameValuePairList = new ArrayList<>();

                nameValuePairList.add(new BasicNameValuePair(Constant.COMMAND_PARAM.jobid, Global.jobId.toString()));
                // 使用internal的原理是clone()函数
                nameValuePairList.add(new BasicNameValuePair(Constant.COMMAND_PARAM.job, JSON.toJSONString(taskGroupConf.getInternal())));
                nameValuePairList.add(new BasicNameValuePair(Constant.COMMAND_PARAM.mode, ExecuteMode.taskGroup.name()));

                String target = HttpUtil.appendUrl(node.host, node.port) + Constant.START_HTTP_PATH;
                try {
                    HttpUtil.post(target, nameValuePairList);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        throw new UnsupportedOperationException();
    }

    @Override
    protected void dealFailedStat(AbstractContainerCommunicator abstractContainerCommunicator, Throwable throwable) {
        throw DataXException.build(FrameworkErrorCode.PLUGIN_RUNTIME_ERROR, throwable);
    }

    @Override
    protected void dealKillingStat(AbstractContainerCommunicator abstractContainerCommunicator, int totalTasks) {
        throw DataXException.build(FrameworkErrorCode.KILLED_EXIT_VALUE);
    }

    private Map<DispatcherInfo, List<Configuration>> dispatchTaskGroupConfList2NodeList(List<Configuration> taskGroupConfList, List<DispatcherInfo> nodeList) {
        // 首先想到的套路是 任务要均匀分摊到各个node
        // 要是node不够的话本节点优先多承担

        Map<DispatcherInfo, List<Configuration>> map = new HashMap<>();

        int taskGroupConfCount = taskGroupConfList.size();
        int nodeCount = nodeList.size();

        int min = Math.min(taskGroupConfCount, nodeCount);

        for (int a = 0; a < min; a++) {
            map.put(nodeList.get(a), Collections.singletonList(taskGroupConfList.get(a)));
        }

        // node数量的不够,要把多的任务倾斜到当前的主node
        // 目前缺陷 远端的node当前还不支持多个taskGroupConf
        if (nodeCount < taskGroupConfCount) {
            List<Configuration> targetList = map.get(Global.localNode);

            for (int a = min; a < taskGroupConfCount; a++) {
                targetList.add(taskGroupConfList.get(a));
            }
        }


        return map;
    }
}
