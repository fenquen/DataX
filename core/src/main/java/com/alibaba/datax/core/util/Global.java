package com.alibaba.datax.core.util;

import com.alibaba.datax.common.distribute.DispatcherInfo;
import com.alibaba.datax.common.constant.Constant;
import com.alibaba.datax.common.util.ExecuteMode;

import java.util.List;

public class Global {
    public static Long jobId;
    public static ExecuteMode mode;

    public static List<DispatcherInfo> nodeList;

    public static String localNodeHost = System.getenv(Constant.ENV_PARAM.localNodeHost);
    public static String localNodePort = System.getenv(Constant.ENV_PARAM.localNodePort);

    public static String masterNodeHost = System.getenv(Constant.ENV_PARAM.masterNodeHost);
    public static String masterNodePort = System.getenv(Constant.ENV_PARAM.masterNodePort);

    public static String masterNodeNettyHttpServerPort = System.getenv(Constant.ENV_PARAM.masterNodeNettyHttpServerPort);
    public static String masterNodeNettyHttpServerAddr = "http://" + masterNodeHost + ":" + masterNodeNettyHttpServerPort;

    public static final DispatcherInfo masterNode = new DispatcherInfo(masterNodeHost, masterNodePort);

    public static Thread nettyThread;

}
