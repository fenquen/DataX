package com.alibaba.datax.core.util;


import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import java.util.List;
import java.util.Map;

public class HttpUtil {
    private static final String UTF_8 = "UTF-8";

    private static CloseableHttpClient HTTP_CLIENT_SYNC;

    public static RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(60000)
            .setSocketTimeout(60000)
            // get connection from pool
            .setConnectionRequestTimeout(1000)
            .build();

    static {
        try {
            HTTP_CLIENT_SYNC = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String postJsonStr(String url, String jsonStr) throws Exception {
        return postStr(url, jsonStr, "application/json");
    }

    private static String postStr(String url,
                                  String str,
                                  String requestContentType) throws Exception {
        HttpPost httpPost = new HttpPost(url);

        StringEntity stringEntity = new StringEntity(str, "utf-8");
        stringEntity.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE, requestContentType));
        httpPost.setEntity(stringEntity);

        httpPost.setHeader("Content-type", requestContentType);
        HttpResponse httpResponse = HTTP_CLIENT_SYNC.execute(httpPost);

        int statusCode = httpResponse.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
            throw new RuntimeException("HttpStatus:" + statusCode);
        }

        return EntityUtils.toString(httpResponse.getEntity());
    }

    public static String post(String url, List<NameValuePair> pairs) throws Exception {
        return post(url, null, pairs);
    }

    public static String post(String url,
                              Map<String, String> headers,
                              List<NameValuePair> pairs) throws Exception {
        HttpPost httpPost = new HttpPost(url);
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                httpPost.addHeader(entry.getKey(), entry.getValue());
            }
        }

        httpPost.setEntity(new UrlEncodedFormEntity(pairs, UTF_8));

        HttpResponse httpResponse = HTTP_CLIENT_SYNC.execute(httpPost);

        int statusCode = httpResponse.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
            throw new RuntimeException("HttpStatus:" + statusCode);
        }

        return EntityUtils.toString(httpResponse.getEntity());
    }

    public static String appendUrl(String host, String port) {
        return "http://" + host + ":" + port;
    }
}
