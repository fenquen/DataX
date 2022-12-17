package com.alibaba.datax.common.constant;

public class Constant {

    public interface SPRING_HTTP {
        String START_HTTP_PATH = "/business/start";
        String STOP_HTTP_PATH = "/business/stop";
    }

    public interface COMMAND_PARAM {
        String jobid = "jobid";
        String job = "job";
        String mode = "mode";
    }

    public static final String EMPTY_STRING = "";

    public interface ENV_PARAM {
        String nodeList = "node_list";

        String masterNodeHost = "masterNodeHost";
        String masterNodePort = "masterNodePort";

        String masterNodeNettyHttpServerPort = "nettyHttpServerPort";


        String localNodeHost = "localNodeHost";
        String localNodePort = "localNodePort";
    }

    public static final int INVALID_ID = -1;


    public interface NETTY_HTTP {
        String REPORT_TG_COMM_PATH = "/netty_http/report_task_group_communication";
    }
}
