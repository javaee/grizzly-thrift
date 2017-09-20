/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2012-2017 Oracle and/or its affiliates. All rights reserved.
 *
 * The contents of this file are subject to the terms of either the GNU
 * General Public License Version 2 only ("GPL") or the Common Development
 * and Distribution License("CDDL") (collectively, the "License").  You
 * may not use this file except in compliance with the License.  You can
 * obtain a copy of the License at
 * https://glassfish.dev.java.net/public/CDDL+GPL_1_1.html
 * or packager/legal/LICENSE.txt.  See the License for the specific
 * language governing permissions and limitations under the License.
 *
 * When distributing the software, include this License Header Notice in each
 * file and include the License file at packager/legal/LICENSE.txt.
 *
 * GPL Classpath Exception:
 * Oracle designates this particular file as subject to the "Classpath"
 * exception as provided by Oracle in the GPL Version 2 section of the License
 * file that accompanied this code.
 *
 * Modifications:
 * If applicable, add the following below the License Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyright [year] [name of copyright owner]"
 *
 * Contributor(s):
 * If you wish your version of this file to be governed by only the CDDL or
 * only the GPL Version 2, indicate your decision by adding "[Contributor]
 * elects to include this software in this distribution under the [CDDL or GPL
 * Version 2] license."  If you don't indicate a single choice of license, a
 * recipient has the option to distribute your version of this file under
 * either the CDDL, the GPL Version 2 or to extend the choice of license to
 * its licensees as provided above.  However, if you add GPL Version 2 code
 * and therefore, elected the GPL Version 2 license, then the option applies
 * only if the new code is made subject to such option by the copyright
 * holder.
 */

package org.glassfish.grizzly.thrift.client.zookeeper;

import org.glassfish.grizzly.Grizzly;
import org.glassfish.grizzly.thrift.client.ThriftClient;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The {@link BarrierListener} implementation for synchronizing the thrift server list among all clients which have joined the same zookeeper server
 *
 * @author Bongjae Chang
 */
public class ServerListBarrierListener implements BarrierListener {

    private static final Logger logger = Grizzly.logger(ServerListBarrierListener.class);
    public static final String DEFAULT_SERVER_LIST_CHARSET = "UTF-8";

    private final String thriftClientName;
    private final ThriftClient thriftClient;
    private final Set<SocketAddress> localServerSet = new CopyOnWriteArraySet<SocketAddress>();
    private final List<BarrierListener> customListenerList = new CopyOnWriteArrayList<BarrierListener>();

    public ServerListBarrierListener(final ThriftClient thriftClient, final Set<SocketAddress> serverSet) {
        this.thriftClient = thriftClient;
        if (this.thriftClient != null) {
            thriftClientName = this.thriftClient.getName();
        } else {
            thriftClientName = null;
        }
        if (serverSet != null) {
            this.localServerSet.addAll(serverSet);
        }
    }

    @Override
    public void onInit(final String regionName, final String path, final byte[] remoteBytes) {
        if (remoteBytes == null || remoteBytes.length == 0) {
            return;
        }
        // check the remote thrift server list of the zookeeper server is equal to local if the server has pre-defined server list
        try {
            final String remoteServerList = new String(remoteBytes, DEFAULT_SERVER_LIST_CHARSET);
            final Set<SocketAddress> remoteServers = getAddressesFromStringList(remoteServerList);
            boolean checked = true;
            for (final SocketAddress local : localServerSet) {
                if (!remoteServers.remove(local)) {
                    checked = false;
                    break;
                }
            }
            if (checked && !remoteServers.isEmpty()) {
                checked = false;
            }
            if (!checked) {
                if (logger.isLoggable(Level.WARNING)) {
                    logger.log(Level.WARNING,
                            "failed to check the thrift server list from the remote. thriftClientName={0}, local={1}, remote={2}",
                            new Object[]{thriftClientName, localServerSet, remoteServers});
                }
            } else {
                if (logger.isLoggable(Level.INFO)) {
                    logger.log(Level.INFO, "thrift server list confirmed. thriftClientName={0}, list=[{1}]", new Object[]{thriftClientName, remoteServerList});
                }
            }
        } catch (UnsupportedEncodingException uee) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.log(Level.WARNING, "failed to check the thrift server list from the remote. thriftClientName=" + thriftClientName, uee);
            }
        } finally {
            for (final BarrierListener listener : customListenerList) {
                try {
                    listener.onInit(regionName, path, remoteBytes);
                } catch (Exception e) {
                    if (logger.isLoggable(Level.WARNING)) {
                        logger.log(Level.WARNING, "failed to call onInit(). thriftClientName=" + thriftClientName + ", listener=" + listener, e);
                    }
                }
            }
        }
    }

    @Override
    public void onCommit(final String regionName, final String path, byte[] remoteBytes) {
        if (remoteBytes == null || remoteBytes.length == 0) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.log(Level.WARNING, "remote bytes is null or NO_DATA(byte[0]). regionName={0}, path={1}", new Object[]{regionName, path});
            }
            return;
        }
        try {
            final String remoteDataString = new String(remoteBytes, DEFAULT_SERVER_LIST_CHARSET);
            final Set<SocketAddress> remoteServers = getAddressesFromStringList(remoteDataString);
            if (!remoteServers.isEmpty()) {
                if (thriftClient != null) {
                    final Set<SocketAddress> shouldBeAdded = new HashSet<SocketAddress>();
                    final Set<SocketAddress> shouldBeRemoved = new HashSet<SocketAddress>();
                    for (final SocketAddress remoteServer : remoteServers) {
                        if (!localServerSet.remove(remoteServer)) {
                            shouldBeAdded.add(remoteServer);
                        }
                    }
                    shouldBeRemoved.addAll(localServerSet);
                    for (final SocketAddress address : shouldBeAdded) {
                        thriftClient.addServer(address);
                    }
                    for (final SocketAddress address : shouldBeRemoved) {
                        thriftClient.removeServer(address);
                    }
                    // refresh local
                    localServerSet.clear();
                    localServerSet.addAll(remoteServers);
                }
            }
        } catch (UnsupportedEncodingException uee) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.log(Level.WARNING,
                        "failed to apply the changed server list of the remote zookeeper server. regionName=" + regionName + ", path=" + path,
                        uee);
            }
        } finally {
            for (final BarrierListener listener : customListenerList) {
                try {
                    listener.onCommit(regionName, path, remoteBytes);
                } catch (Exception e) {
                    if (logger.isLoggable(Level.WARNING)) {
                        logger.log(Level.WARNING, "failed to call onCommit(). thriftClientName=" + thriftClientName + ", listener=" + listener, e);
                    }
                }
            }
        }
    }

    @Override
    public void onDestroy(final String regionName) {
        for (final BarrierListener listener : customListenerList) {
            try {
                listener.onDestroy(regionName);
            } catch (Exception e) {
                if (logger.isLoggable(Level.WARNING)) {
                    logger.log(Level.WARNING, "failed to call onDestroy(). thriftClientName=" + thriftClientName + ", listener=" + listener, e);
                }
            }
        }
    }

    public void addCustomListener(final BarrierListener listener) {
        if (listener == null) {
            return;
        }
        customListenerList.add(listener);
    }

    public void removeCustomListener(final BarrierListener listener) {
        if (listener == null) {
            return;
        }
        customListenerList.remove(listener);
    }

    @Override
    public String toString() {
        return "ServerListBarrierListener{" +
                "thriftClientName='" + thriftClientName + '\'' +
                ", thriftClient=" + thriftClient +
                ", localServerSet=" + localServerSet +
                ", customListenerList=" + customListenerList +
                '}';
    }

    /**
     * Split a string in the form of "host:port, host2:port" into a Set of
     * {@link java.net.SocketAddress} instances.
     * <p>
     * Note that colon-delimited IPv6 is also supported. For example: ::1:11211
     *
     * @param serverList server list in the form of "host:port,host2:port"
     * @return server set
     */
    public static Set<SocketAddress> getAddressesFromStringList(final String serverList) {
        if (serverList == null) {
            throw new IllegalArgumentException("null host list");
        }
        if (serverList.trim().equals("")) {
            throw new IllegalArgumentException("no hosts in list:  ``" + serverList + "''");
        }
        final HashSet<SocketAddress> addrs = new HashSet<SocketAddress>();
        for (final String hoststuff : serverList.split("(,| )")) {
            if (hoststuff.length() == 0) {
                continue;
            }
            int finalColon = hoststuff.lastIndexOf(':');
            if (finalColon < 1) {
                throw new IllegalArgumentException("Invalid server ``" + hoststuff + "'' in list:  " + serverList);
            }
            final String hostPart = hoststuff.substring(0, finalColon);
            final String portNum = hoststuff.substring(finalColon + 1);
            addrs.add(new InetSocketAddress(hostPart, Integer.parseInt(portNum)));
        }
        return addrs;
    }

    /**
     * Convert server set into server list like "host:port,host2:port"
     *
     * @param servers {@link java.net.InetSocketAddress} set
     * @return server list in the form of "host:port,host2:port"
     */
    public static String getStringListFromAddressSet(final Set<SocketAddress> servers) {
        if (servers == null || servers.isEmpty()) {
            throw new IllegalArgumentException("Null servers");
        }
        final StringBuilder builder = new StringBuilder(256);
        for (final SocketAddress server : servers) {
            if (server instanceof InetSocketAddress) {
                final InetSocketAddress inetSocketAddress = (InetSocketAddress) server;
                builder.append(inetSocketAddress.getHostName()).append(':').append(inetSocketAddress.getPort());
                builder.append(',');
            }
        }
        final String result = builder.toString();
        final int resultLength = result.length();
        if (resultLength < 1) {
            throw new IllegalArgumentException("there is no InetSocketAddress in the server set");
        } else {
            // remove the last comma
            return result.substring(0, result.length() - 1);
        }
    }
}
