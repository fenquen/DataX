package com.alibaba.datax.core.transport.exchanger;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.channel.Channel;
import com.alibaba.datax.core.transport.record.TerminateRecord;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

// writer和reader各有自己的
public class BufferedRecordExchanger implements RecordSender, RecordReceiver {

    private final Channel channel;

    private final Configuration configuration;

    private final List<Record> buffer;

    private int bufferSize;

    protected final int channelByteCapacity;

    private final AtomicInteger memoryUsed = new AtomicInteger(0);

    private int bufferIndex = 0;

    private static Class<? extends Record> RECORD_CLASS;

    private volatile boolean shutdown = false;

    private final TaskPluginCollector pluginCollector;

    @SuppressWarnings("unchecked")
    public BufferedRecordExchanger(Channel channel, TaskPluginCollector pluginCollector) {
        assert null != channel;
        assert null != channel.getConfiguration();

        this.channel = channel;
        this.pluginCollector = pluginCollector;
        configuration = channel.getConfiguration();

        bufferSize = configuration.getInt(CoreConstant.DATAX_CORE_TRANSPORT_EXCHANGER_BUFFERSIZE);
        buffer = new ArrayList<>(bufferSize);

        channelByteCapacity = configuration.getInt(CoreConstant.DATAX_CORE_TRANSPORT_CHANNEL_CAPACITY_BYTE, 8 * 1024 * 1024);

        try {
            BufferedRecordExchanger.RECORD_CLASS =
                    ((Class<? extends Record>) Class.forName(configuration.getString(CoreConstant.DATAX_CORE_TRANSPORT_RECORD_CLASS, "com.alibaba.datax.core.transport.record.DefaultRecord")));
        } catch (Exception e) {
            throw DataXException.build(FrameworkErrorCode.CONFIG_ERROR, e);
        }
    }

    @Override
    public Record createRecord() {
        try {
            return BufferedRecordExchanger.RECORD_CLASS.newInstance();
        } catch (Exception e) {
            throw DataXException.build(FrameworkErrorCode.CONFIG_ERROR, e);
        }
    }

    @Override
    public void sendToWriter(Record record) {
        if (shutdown) {
            throw DataXException.build(CommonErrorCode.SHUT_DOWN_TASK, "");
        }

        Validate.notNull(record, "record不能为空.");

        if (record.getMemorySize() > channelByteCapacity) {
            pluginCollector.collectDirtyRecord(record, new Exception(String.format("单条记录超过大小限制，当前限制为:%s", channelByteCapacity)));
            return;
        }

        boolean isFull = (bufferIndex >= bufferSize || memoryUsed.get() + record.getMemorySize() > channelByteCapacity);
        if (isFull) {
            flush();
        }

        buffer.add(record);
        bufferIndex++;

        memoryUsed.addAndGet(record.getMemorySize());
    }

    @Override
    public void flush() {
        if (shutdown) {
            throw DataXException.build(CommonErrorCode.SHUT_DOWN_TASK, "");
        }

        channel.pushAll(buffer);

        buffer.clear();
        bufferIndex = 0;

        memoryUsed.set(0);
    }

    @Override
    public void terminate() {
        if (shutdown) {
            throw DataXException.build(CommonErrorCode.SHUT_DOWN_TASK);
        }
        flush();
        this.channel.pushTerminate(TerminateRecord.get());
    }

    @Override
    public Record getFromReader() {
        if (shutdown) {
            throw DataXException.build(CommonErrorCode.SHUT_DOWN_TASK);
        }

        boolean isEmpty = (bufferIndex >= buffer.size());
        if (isEmpty) {
            receive();
        }

        Record record = buffer.get(bufferIndex++);
        if (record instanceof TerminateRecord) {
            record = null;
        }

        return record;
    }

    @Override
    public void shutdown() {
        shutdown = true;
        try {
            buffer.clear();
            channel.clear();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private void receive() {
        channel.pullAll(buffer);
        bufferIndex = 0;
        bufferSize = buffer.size();
    }
}
