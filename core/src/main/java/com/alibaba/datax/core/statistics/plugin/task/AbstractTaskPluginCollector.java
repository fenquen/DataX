package com.alibaba.datax.core.statistics.plugin.task;

import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.FrameworkErrorCode;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jingxing on 14-9-11.
 */
@Getter
public abstract class AbstractTaskPluginCollector extends TaskPluginCollector {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractTaskPluginCollector.class);

    private Communication communication;

    private Configuration configuration;

    private PluginType pluginType;

    public AbstractTaskPluginCollector(Configuration conf, Communication communication, PluginType type) {
        this.configuration = conf;
        this.communication = communication;
        this.pluginType = type;
    }

    @Override
    final public void collectMessage(String key, String value) {
        communication.addMessage(key, value);
    }

    @Override
    public void collectDirtyRecord(Record dirtyRecord, Throwable t, String errorMessage) {

        if (null == dirtyRecord) {
            LOG.warn("脏数据record=null.");
            return;
        }

        if (pluginType.equals(PluginType.READER)) {
            communication.increaseCounter(CommunicationTool.READ_FAILED_RECORDS, 1);
            communication.increaseCounter(CommunicationTool.READ_FAILED_BYTES, dirtyRecord.getByteSize());
        } else if (this.pluginType.equals(PluginType.WRITER)) {
            communication.increaseCounter(CommunicationTool.WRITE_FAILED_RECORDS, 1);
            communication.increaseCounter(CommunicationTool.WRITE_FAILED_BYTES, dirtyRecord.getByteSize());
        } else {
            throw DataXException.build(FrameworkErrorCode.RUNTIME_ERROR, String.format("不知道的插件类型[%s].", pluginType));
        }
    }
}
