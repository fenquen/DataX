package com.alibaba.datax.core.statistics.communication;

import com.alibaba.datax.common.base.BaseObject;
import com.alibaba.datax.common.constant.State;
import com.alibaba.datax.core.util.Global;
import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DataX所有的状态及统计信息交互类，job、taskGroup、task等的消息汇报都走该类
 */
@Data
public class Communication extends BaseObject implements Cloneable {

    /**
     * 隶属对应的task group,如果是task group的communication的话
     */
    public int taskGroupId;

    /**
     * 在哪个node上运行的
     */
    public String nodeHost = Global.localNodeHost;
    public String nodePort = Global.localNodePort;

    /**
     * 记录各个的用来汇总的task group的communication情况
     */
    public Map<State, List<String>> taskGroupState_taskGroupNodeHostList = new HashMap<>();

    /**
     * 所有的数值key-value对
     */
    private Map<String, Number> key_number;

    /**
     * 运行状态
     */
    private State state;

    /**
     * 异常记录
     */
    private Throwable throwable;

    /**
     * 记录的timestamp
     */
    private long timestamp;

    /**
     * task给job的信息
     */
    Map<String, List<String>> id_messageList;

    public Communication() {
        init();
    }

    public synchronized void reset() {
        init();
    }

    private void init() {
        this.key_number = new ConcurrentHashMap<>();
        this.state = State.RUNNING;
        this.throwable = null;
        this.id_messageList = new ConcurrentHashMap<>();
        this.timestamp = System.currentTimeMillis();
    }

    public void setState(State state, boolean isForce) {
        synchronized (this) {
            if (!isForce && this.state.equals(State.FAILED)) {
                return;
            }

            this.state = state;
        }
    }

    public void setThrowable(Throwable throwable) {
        setThrowable(throwable, false);
    }

    public synchronized void setThrowable(Throwable throwable, boolean isForce) {
        if (isForce) {
            this.throwable = throwable;
        } else {
            this.throwable = this.throwable == null ? throwable : this.throwable;
        }
    }

    public synchronized String getThrowableMessage() {
        return throwable == null ? "" : throwable.getMessage();
    }

    public List<String> getMessageListByKey(String key) {
        return id_messageList.get(key);
    }

    public synchronized void addMessage(String key, String value) {
        Validate.isTrue(StringUtils.isNotBlank(key), "增加message的key不能为空");
        List<String> valueList = id_messageList.computeIfAbsent(key, k -> new ArrayList<>());

        valueList.add(value);
    }

    public synchronized Long getLongCounter(final String key) {
        Number value = this.key_number.get(key);
        return value == null ? 0 : value.longValue();
    }

    public synchronized void setLongCounter(String key, long value) {
        Validate.isTrue(StringUtils.isNotBlank(key), "设置counter的key不能为空");
        key_number.put(key, value);
    }

    public synchronized Double getDoubleCounter(final String key) {
        Number value = key_number.get(key);
        return value == null ? 0.0d : value.doubleValue();
    }

    public synchronized void setDoubleCounter(final String key, final double value) {
        Validate.isTrue(StringUtils.isNotBlank(key), "设置counter的key不能为空");
        this.key_number.put(key, value);
    }

    public synchronized void increaseCounter(String key, final long deltaValue) {
        Validate.isTrue(StringUtils.isNotBlank(key), "增加counter的key不能为空");

        long value = this.getLongCounter(key);
        key_number.put(key, value + deltaValue);
    }

    @Override
    public Communication clone() {
        Communication communication = new Communication();

        // clone counter
        if (this.key_number != null) {
            for (Map.Entry<String, Number> entry : this.key_number.entrySet()) {
                String key = entry.getKey();
                Number value = entry.getValue();
                if (value instanceof Long) {
                    communication.setLongCounter(key, (Long) value);
                } else if (value instanceof Double) {
                    communication.setDoubleCounter(key, (Double) value);
                }
            }
        }

        communication.setState(state, true);
        communication.setThrowable(throwable, true);
        communication.setTimestamp(timestamp);

        // clone message
        if (this.id_messageList != null) {
            for (final Map.Entry<String, List<String>> entry : this.id_messageList.entrySet()) {
                String key = entry.getKey();
                List<String> value = new ArrayList<String>(entry.getValue()) {{
                    addAll(entry.getValue());
                }};
                communication.getId_messageList().put(key, value);
            }
        }

        return communication;
    }

    /**
     * 合并后的整个的更新时间其实的话用不着管,this便是最新的底
     */
    public synchronized void mergeFrom(Communication otherComm) {
        if (otherComm == null) {
            return;
        }

        // counter的合并，将otherComm的值累加到this中，不存在的则创建同为long
        for (Entry<String, Number> entry : otherComm.getKey_number().entrySet()) {
            String key = entry.getKey();
            Number otherValue = entry.getValue();
            if (otherValue == null) {
                continue;
            }

            Number value = key_number.get(key);
            if (value == null) {
                value = otherValue;
            } else {
                if ("stage".equals(key)) {
                    System.out.println(value.getClass());
                    System.out.println(otherValue.getClass());
                }

                boolean value整数 = value instanceof Long || value instanceof Integer;
                boolean otherValue整数 = otherValue instanceof Long || otherValue instanceof Integer;
                // 处理stage的时候因为远端的communication是json反序列化过来的 long变成了int
                if (value整数 && otherValue整数) {
                    value = value.longValue() + otherValue.longValue();
                } else {
                    value = value.doubleValue() + otherValue.doubleValue();
                }
            }

            key_number.put(key, value);
        }

        // 合并state
        mergeStateFrom(otherComm);

        // 合并throwable，当this.throwable为空时将otherComm的throwable合并进来

        throwable = throwable == null ? otherComm.getThrowable() : throwable;

        // message的合并采取求并的方式即全部累计在一起
        for (Entry<String, List<String>> entry : otherComm.getId_messageList().entrySet()) {
            String key = entry.getKey();
            List<String> valueList = id_messageList.computeIfAbsent(key, key1 -> new ArrayList<>());
            valueList.addAll(entry.getValue());
        }

    }

    /**
     * 合并state，优先级： (Failed | Killed) > Running > Success
     * 这里不会出现 Killing 状态，killing 状态只在 Job 自身状态上才有.
     */
    public synchronized State mergeStateFrom(final Communication otherComm) {
        State retState = state;
        if (otherComm == null) {
            return retState;
        }

        if (state == State.FAILED || otherComm.getState() == State.FAILED ||
                state == State.KILLED || otherComm.getState() == State.KILLED) {
            retState = State.FAILED;
        } else if (state.isRunning() || otherComm.state.isRunning()) {
            retState = State.RUNNING;
        }

        state = retState;

        return retState;
    }

    public synchronized boolean isFinished() {
        return state.isFinished();
    }

    public synchronized boolean isRunning() {
        return state.isRunning();
    }
}
