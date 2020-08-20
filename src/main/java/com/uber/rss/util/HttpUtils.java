/*
 * Copyright (c) 2020 Uber Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
