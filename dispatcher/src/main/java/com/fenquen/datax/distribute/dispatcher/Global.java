package com.fenquen.datax.distribute.dispatcher;

import com.alibaba.datax.common.distribute.DispatcherInfo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Global {
    public static final Map<String, DispatcherInfo> HOST_PORT_DISPATCHER_INFO = new ConcurrentHashMap<>();

    public static final Map<String, Process> JOB_ID_PROCESS = new ConcurrentHashMap<>();
}
