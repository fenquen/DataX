package com.fenquen.datax.distribute.dispatcher;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Global {
    public static final Map<String, DispatcherInfo> HOST_PORT_DISPATCHER_INFO = new ConcurrentHashMap<>();
}
