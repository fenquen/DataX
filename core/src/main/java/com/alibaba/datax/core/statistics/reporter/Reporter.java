package com.alibaba.datax.core.statistics.reporter;

import com.alibaba.datax.common.constant.Constant;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.LocalTGCommunicationManager;
import com.alibaba.datax.core.util.Global;
import com.alibaba.datax.core.util.HttpUtil;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(Reporter.class);

    public void reportJobCommunication(Long jobId, Communication communication) {

    }

    /**
     * 把 taskGroup维度的 communication 上报汇总
     */
    public void reportTGCommunication(Integer taskGroupId, Communication communication) {
        communication.taskGroupId = taskGroupId;
        switch (Global.mode) {
            case local:
            case standalone:
            case distribute: // 说明当前是主node
                LocalTGCommunicationManager.update(taskGroupId, communication);
                break;
            case taskGroup: // 需要上报
                try {
                    HttpUtil.postJsonStr(Global.masterNodeNettyHttpServerAddr + Constant.NETTY_HTTP.REPORT_TG_COMM_PATH, JSON.toJSONString(communication));
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
        }
    }
}
