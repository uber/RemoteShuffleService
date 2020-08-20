package com.uber.rss;

import com.uber.rss.exceptions.RssException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class RssBuildInfo {
    public static String UnknownValue = "<unknown>";

    public static String Version;
    public static String Revision;

    static {
        String fileName = "rss-build-info.properties";
        try (InputStream resourceStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)) {
            if (resourceStream == null) {
                throw new RssException(String.format("Could not find info file: %s", fileName));
            }

            Properties props = new Properties();
            props.load(resourceStream);

            Version = props.getProperty("version", UnknownValue);
            Revision = props.getProperty("revision", UnknownValue);
        } catch (IOException e) {
            throw new RssException(String.format("Failed to open info file: %s", fileName));
        }
    }
}
