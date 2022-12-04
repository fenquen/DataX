package com.alibaba.datax.core.container;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communicator.AbstractContainerCommunicator;
import lombok.Data;

/**
 * 执行容器的抽象类，持有该容器全局的配置 configuration
 */
@Data
public abstract class AbstractContainer {
    protected Configuration configuration;

    protected AbstractContainerCommunicator abstractContainerCommunicator;

    public AbstractContainer(Configuration configuration) {
        this.configuration = configuration;
    }

    public abstract void start();

}
