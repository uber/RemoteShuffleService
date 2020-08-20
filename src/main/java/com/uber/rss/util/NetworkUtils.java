package com.uber.rss.util;

import com.google.common.net.HostAndPort;
import com.google.common.net.InetAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

public class NetworkUtils {
    public static final int DEFAULT_REACHABLE_TIMEOUT = 10000;

    private static final Logger logger = LoggerFactory.getLogger(NetworkUtils.class);

    public static int getAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find available port to use", e);
        }
    }

    public static String getLocalHostName() {
        try {
            Map<String, String> env = System.getenv();
            if (env.containsKey("COMPUTERNAME"))
                return env.get("COMPUTERNAME");
            else if (env.containsKey("HOSTNAME"))
                return env.get("HOSTNAME");
            else
                return InetAddress.getLocalHost().getHostName();
        } catch (Throwable e) {
            return "localhost";
        }
    }

    // We use FQDN to register shuffle server because some data center (e.g. ATG) needs FQDN to resolve the server to ip address.
    public static String getLocalFQDN() {
        InetAddress address;
        try {
            address = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            // Never going to happen...
            throw new RuntimeException("Unable to fetch address details for localhost", e);
        }
        return address.getCanonicalHostName();
    }

    public static boolean isReachable(String host, int timeout) {
        if (host == null || host.isEmpty()) {
            return false;
        }

        try {
            InetAddress inetAddress;
            if (InetAddresses.isInetAddress(host)) {
                inetAddress = InetAddresses.forString(host);
            } else {
                inetAddress = InetAddress.getByName(host);
            }
            return inetAddress.isReachable(timeout);
        } catch (IOException ex) {
            logger.warn(String.format("Host %s not reachable due to %s", host, ExceptionUtils.getSimpleMessage(ex)), ex);
            return false;
        }
    }
}
