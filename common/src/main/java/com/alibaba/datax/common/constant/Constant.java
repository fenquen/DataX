package com.alibaba.datax.common.constant;

public class Constant {
    public static final String START_HTTP_PATH = "/business/start";

    public interface COMMAND_PARAM {
        String jobid = "jobid";
        String job = "job";
        String mode = "mode";
    }

    public static final String EMPTY_STRING = "";

    public interface ENV_PARAM {
        String nodeList = "node_list";

        String masterNodeHost = "selfHost";
        String masterNodePort = "selfPort";

        String masterNodeNettyHttpServerPort = "nettyHttpServerPort";
    }

    public static final int INVALID_ID = -1;


    public interface NETTY_HTTP {
        String REPORT_TG_COMM_PATH = "/netty_http/report_task_group_communication";
    }
}
