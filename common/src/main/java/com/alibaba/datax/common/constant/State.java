package com.alibaba.datax.common.constant;

public enum State {

    SUBMITTING(10),
    WAITING(20),
    RUNNING(30),
    KILLING(40),
    KILLED(50),
    FAILED(60),
    SUCCEEDED(70);


    /* 一定会被初始化的 */
    int value;

    State(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }


    public boolean isFinished() {
        return this == KILLED || this == FAILED || this == SUCCEEDED;
    }

    public boolean isRunning() {
        return !isFinished();
    }

}