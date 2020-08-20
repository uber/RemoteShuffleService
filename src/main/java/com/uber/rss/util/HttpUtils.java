package com.uber.rss.util;

import com.uber.rss.exceptions.RssException;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.io.IOUtils;

public class HttpUtils {
    private final static MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
    
    public static String getUrl(String url) {
        HttpClient client = new HttpClient(connectionManager);

        HttpMethod method = new GetMethod(url);
        try {
            client.executeMethod(method);

            if (method.getStatusCode() != 200) {
                throw new RssException(String.format(
                        "Failed to get url %s, response %s, %s",
                        url, method.getStatusCode(), IOUtils.toString(method.getResponseBodyAsStream())));
            }

            return IOUtils.toString(method.getResponseBodyAsStream());
        } catch (RssException e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException("Failed to get url " + url, e);
        } finally {
            method.releaseConnection();
        }
    }
}
