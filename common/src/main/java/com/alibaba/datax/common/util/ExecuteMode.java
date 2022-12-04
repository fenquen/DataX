package com.alibaba.datax.common.util;

public enum ExecuteMode{
    standalone,

    local,

    distribute,

    /**
     * 该模式下应该直接到本地运行版本的startAllTaskGroup
     */
    taskGroup;
}
