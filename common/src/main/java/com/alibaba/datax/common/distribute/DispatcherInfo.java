package com.alibaba.datax.common.distribute;

import java.util.Objects;

public class DispatcherInfo {
    // tomcat的
    public String host;

    public String port;

    public DispatcherInfo(String host, String port) {
        this.host = host;
        this.port = port;
    }

    public DispatcherInfo() {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DispatcherInfo that = (DispatcherInfo) o;
        return Objects.equals(host, that.host) && Objects.equals(port, that.port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }
}
