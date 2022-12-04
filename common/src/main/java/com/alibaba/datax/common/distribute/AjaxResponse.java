package com.alibaba.datax.common.distribute;

import com.alibaba.fastjson.JSON;

import java.io.PrintWriter;
import java.io.StringWriter;

public class AjaxResponse {
    public static final AjaxResponse SUCCESS = AjaxResponse.success(null);

    public boolean success;
    public String message;
    public String stack;
    public Object data;

    public boolean needLogin;
    public boolean forbidden;

    public static AjaxResponse fail(Throwable throwable) {
        AjaxResponse ajaxResponse = new AjaxResponse();
        ajaxResponse.success = false;
        StringWriter stringWriter = new StringWriter(1024);
        throwable.printStackTrace(new PrintWriter(stringWriter));
        ajaxResponse.stack = stringWriter.toString();
        ajaxResponse.message = throwable.getMessage();
        return ajaxResponse;
    }

    public static AjaxResponse fail(String errMsg) {
        AjaxResponse ajaxResponse = new AjaxResponse();
        ajaxResponse.success = false;
        ajaxResponse.message = errMsg;
        return ajaxResponse;
    }

    public static AjaxResponse success(Object data) {
        AjaxResponse ajaxResponse = new AjaxResponse();
        ajaxResponse.success = true;
        ajaxResponse.data = data;
        return ajaxResponse;
    }

    public static AjaxResponse needLogin(String message) {
        AjaxResponse ajaxResponse = new AjaxResponse();
        ajaxResponse.success = false;
        ajaxResponse.needLogin = true;
        ajaxResponse.message = message;
        return ajaxResponse;
    }

    public static AjaxResponse forbidden(String message) {
        AjaxResponse ajaxResponse = new AjaxResponse();
        ajaxResponse.success = false;
        ajaxResponse.forbidden = true;
        ajaxResponse.message = message;
        return ajaxResponse;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
