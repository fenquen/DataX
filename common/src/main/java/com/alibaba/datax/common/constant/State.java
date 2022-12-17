package com.alibaba.datax.common.constant;

/**
 * 目前使用的只有 failed running succeeded
 */
public enum State {
    TASK_GROUP_TIME_OUT(80),

    //  SUBMITTING(10),
    //  WAITING(20),

    RUNNING(30),

    // KILLING(40),
    KILLED(50),
    FAILED(60),
    SUCCEEDED(70);

    private int value;

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