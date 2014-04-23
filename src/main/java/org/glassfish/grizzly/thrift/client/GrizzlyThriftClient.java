/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2012-2014 Oracle and/or its affiliates. All rights reserved.
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

package org.glassfish.grizzly.thrift.client;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.ConnectorHandler;
import org.glassfish.grizzly.Grizzly;
import org.glassfish.grizzly.Processor;
import org.glassfish.grizzly.attributes.Attribute;
import org.glassfish.grizzly.attributes.AttributeHolder;
import org.glassfish.grizzly.filterchain.FilterChain;
import org.glassfish.grizzly.filterchain.FilterChainBuilder;
import org.glassfish.grizzly.filterchain.TransportFilter;
import org.glassfish.grizzly.http.HttpClientFilter;
import org.glassfish.grizzly.thrift.TGrizzlyClientTransport;
import org.glassfish.grizzly.thrift.TTimedoutException;
import org.glassfish.grizzly.thrift.ThriftClientFilter;
import org.glassfish.grizzly.thrift.ThriftFrameFilter;
import org.glassfish.grizzly.thrift.client.pool.BaseObjectPool;
import org.glassfish.grizzly.thrift.client.pool.NoValidObjectException;
import org.glassfish.grizzly.thrift.client.pool.ObjectPool;
import org.glassfish.grizzly.thrift.client.pool.PoolExhaustedException;
import org.glassfish.grizzly.thrift.client.pool.PoolableObjectFactory;
import org.glassfish.grizzly.thrift.client.zookeeper.BarrierListener;
import org.glassfish.grizzly.thrift.client.zookeeper.ServerListBarrierListener;
import org.glassfish.grizzly.thrift.client.zookeeper.ZKClient;
import org.glassfish.grizzly.thrift.client.zookeeper.ZooKeeperSupportThriftClient;
import org.glassfish.grizzly.nio.transport.TCPNIOConnectorHandler;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.thrift.http.ThriftHttpClientFilter;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The implementation of the {@link ThriftClient} based on Grizzly
 * <p/>
 * Basically, this class use {@link BaseObjectPool} for pooling connections of the thrift server
 * and {@link RoundRobinStore} for selecting the thrift server.
 * <p/>
 * When a thrift operation is called,
 * 1. finding the correct server by round-robin
 * 2. borrowing the connection from the connection pool
 * 3. returning the connection to the pool
 * <p/>
 * For the failback of the thrift server, {@link HealthMonitorTask} will be scheduled by {@code healthMonitorIntervalInSecs}.
 * If connecting and writing are failed, this thrift client retries failure operations by {@code retryCount}.
 * The retrial doesn't request failed server but another thrift server.
 * And this client provides {@code failover} flag which can turn off the failover/failback.
 * <p/>
 * Example of use:
 * {@code
 * // creates a ThriftClientManager
 * final GrizzlyThriftClientManager manager = new GrizzlyThriftClientManager.Builder().build();
 *
 * // creates a ThriftClientBuilder
 * final GrizzlyThriftClient.Builder<Calculator.Client> builder = manager.createThriftClientBuilder("Calculator", new Calculator.Client.Factory());
 * // sets initial servers
 * builder.servers(initServerSet);
 * // creates the specific thrift client
 * final ThriftClient<Calculator.Client> calculatorThriftClient = builder.build();
 *
 * // if you need to add another server
 * calculatorThriftClient.addServer(anotherServerAddress);
 *
 * // custom thrift operations
 * Integer result = calculatorThriftClient.execute(new ThriftClientCallback<Calculator.Client, Integer>() {
 *         @Override
 *         public Integer call(Calculator.Client client) throws TException {
 *              return client.add(1, 2);
 *         }
 *     });
 * // ...
 *
 * // shuts down
 * manager.shutdown();
 * }
 *
 * @author Bongjae Chang
 */
public class GrizzlyThriftClient<T extends TServiceClient> implements ThriftClient<T>, ZooKeeperSupportThriftClient {

    private static final Logger logger = Grizzly.logger(GrizzlyThriftClient.class);

    private final String thriftClientName;
    private final TCPNIOTransport transport;
    private final long connectTimeoutInMillis;
    private final long writeTimeoutInMillis;
    private final long responseTimeoutInMillis;
    private final String validationCheckMethodName;

    public static final String CONNECTION_POOL_ATTRIBUTE_NAME = "GrizzlyThriftClient.ConnectionPool";
    public static final String CLIENT_ATTRIBUTE_NAME = "GrizzlyThriftClient.Client";
    private final Attribute<ObjectPool<SocketAddress, T>> connectionPoolAttribute =
            Grizzly.DEFAULT_ATTRIBUTE_BUILDER.createAttribute(CONNECTION_POOL_ATTRIBUTE_NAME);
    private final Attribute<T> clientAttribute =
            Grizzly.DEFAULT_ATTRIBUTE_BUILDER.createAttribute(CLIENT_ATTRIBUTE_NAME);

    private final ObjectPool<SocketAddress, T> connectionPool;

    private final Set<SocketAddress> initialServers;

    private final long healthMonitorIntervalInSecs;
    private final ScheduledFuture<?> scheduledFuture;
    private final HealthMonitorTask healthMonitorTask;
    private final ScheduledExecutorService scheduledExecutor;

    private final boolean retainLastServer;
    private final boolean failover;
    private final int retryCount;

    private final ThriftProtocols thriftProtocol;
    private final TServiceClientFactory<T> clientFactory;

    private final RoundRobinStore<SocketAddress> roundRobinStore = new RoundRobinStore<SocketAddress>();

    private final ZKClient zkClient;
    private final ServerListBarrierListener zkListener;
    private String zooKeeperServerListPath;

    private enum TransferProtocols {
        BASIC, HTTP
    }

    private final TransferProtocols transferProtocol;
    private final int maxThriftFrameLength;
    private final String httpUriPath;
    private final Processor processor;

    private GrizzlyThriftClient(Builder<T> builder) {
        this.thriftClientName = builder.thriftClientName;
        this.transport = builder.transport;
        this.clientFactory = builder.clientFactory;
        this.thriftProtocol = builder.thriftProtocol;
        this.connectTimeoutInMillis = builder.connectTimeoutInMillis;
        this.writeTimeoutInMillis = builder.writeTimeoutInMillis;
        this.responseTimeoutInMillis = builder.responseTimeoutInMillis;
        this.healthMonitorIntervalInSecs = builder.healthMonitorIntervalInSecs;
        this.validationCheckMethodName = builder.validationCheckMethodName;
        this.retainLastServer = builder.retainLastServer;

        this.maxThriftFrameLength = builder.maxThriftFrameLength;
        this.transferProtocol = builder.transferProtocol;
        this.httpUriPath = builder.httpUriPath;
        final FilterChainBuilder clientFilterChainBuilder = FilterChainBuilder.stateless();
        switch (transferProtocol) {
            case HTTP:
                clientFilterChainBuilder.add(new TransportFilter()).add(new HttpClientFilter()).add(new ThriftHttpClientFilter(httpUriPath)).add(new ThriftClientFilter());
                break;
            case BASIC:
            default:
                clientFilterChainBuilder.add(new TransportFilter()).add(new ThriftFrameFilter(maxThriftFrameLength)).add(new ThriftClientFilter());
                break;
        }
        this.processor = clientFilterChainBuilder.build();

        @SuppressWarnings("unchecked")
        final BaseObjectPool.Builder<SocketAddress, T> connectionPoolBuilder =
                new BaseObjectPool.Builder<SocketAddress, T>(new PoolableObjectFactory<SocketAddress, T>() {
                    @Override
                    public T createObject(final SocketAddress key) throws Exception {
                        final ConnectorHandler<SocketAddress> connectorHandler =
                                TCPNIOConnectorHandler.builder(transport).processor(processor).setReuseAddress(true).build();
                        final Future<Connection> future = connectorHandler.connect(key);
                        final Connection<SocketAddress> connection;
                        try {
                            if (connectTimeoutInMillis < 0) {
                                connection = future.get();
                            } else {
                                connection = future.get(connectTimeoutInMillis, TimeUnit.MILLISECONDS);
                            }
                        } catch (InterruptedException ie) {
                            if (!future.cancel(false) && future.isDone()) {
                                final Connection c = future.get();
                                if (c != null && c.isOpen()) {
                                    c.closeSilently();
                                }
                            }
                            if (logger.isLoggable(Level.FINER)) {
                                logger.log(Level.FINER, "failed to get the connection. address=" + key, ie);
                            }
                            throw ie;
                        } catch (ExecutionException ee) {
                            if (!future.cancel(false) && future.isDone()) {
                                final Connection c = future.get();
                                if (c != null && c.isOpen()) {
                                    c.closeSilently();
                                }
                            }
                            if (logger.isLoggable(Level.FINER)) {
                                logger.log(Level.FINER, "failed to get the connection. address=" + key, ee);
                            }
                            throw ee;
                        } catch (TimeoutException te) {
                            if (!future.cancel(false) && future.isDone()) {
                                final Connection c = future.get();
                                if (c != null && c.isOpen()) {
                                    c.closeSilently();
                                }
                            }
                            if (logger.isLoggable(Level.FINER)) {
                                logger.log(Level.FINER, "failed to get the connection. address=" + key, te);
                            }
                            throw te;
                        }
                        if (connection != null) {
                            connectionPoolAttribute.set(connection, connectionPool);
                            final TGrizzlyClientTransport ttransport = TGrizzlyClientTransport.create(connection, responseTimeoutInMillis, writeTimeoutInMillis);
                            final TProtocol protocol;
                            if (thriftProtocol == ThriftProtocols.BINARY) {
                                protocol = new TBinaryProtocol(ttransport);
                            } else if (thriftProtocol == ThriftProtocols.COMPACT) {
                                protocol = new TCompactProtocol(ttransport);
                            } else {
                                protocol = new TBinaryProtocol(ttransport);
                            }
                            final T result = clientFactory.getClient(protocol);
                            clientAttribute.set(connection, result);
                            return result;
                        } else {
                            throw new IllegalStateException("connection must not be null");
                        }
                    }

                    @Override
                    public void destroyObject(final SocketAddress key, final T value) throws Exception {
                        if (value != null) {
                            final TProtocol inputTProtocol = value.getInputProtocol();
                            if (inputTProtocol != null) {
                                final TTransport inputTTransport = inputTProtocol.getTransport();
                                closeTTransport(inputTTransport);
                            }
                            final TProtocol outputTProtocol = value.getOutputProtocol();
                            if (outputTProtocol != null) {
                                final TTransport outputTTransport = outputTProtocol.getTransport();
                                closeTTransport(outputTTransport);
                            }
                        }
                    }

                    private void closeTTransport(final TTransport tTransport) {
                        if (tTransport == null) {
                            return;
                        }
                        if (tTransport instanceof TGrizzlyClientTransport) {
                            final TGrizzlyClientTransport tGrizzlyClientTransport = (TGrizzlyClientTransport) tTransport;
                            final Connection conn = tGrizzlyClientTransport.getGrizzlyConnection();
                            if (conn != null) {
                                AttributeHolder attributeHolder = conn.getAttributes();
                                if (attributeHolder != null) {
                                    attributeHolder.removeAttribute(CONNECTION_POOL_ATTRIBUTE_NAME);
                                }
                                attributeHolder = conn.getAttributes();
                                if (attributeHolder != null) {
                                    attributeHolder.removeAttribute(CLIENT_ATTRIBUTE_NAME);
                                }
                            }
                        }
                        tTransport.close();
                    }

                    @Override
                    public boolean validateObject(final SocketAddress key, final T value) throws Exception {
                        return GrizzlyThriftClient.this.validateClient(value);
                    }
                });
        connectionPoolBuilder.min(builder.minConnectionPerServer);
        connectionPoolBuilder.max(builder.maxConnectionPerServer);
        connectionPoolBuilder.keepAliveTimeoutInSecs(builder.keepAliveTimeoutInSecs);
        connectionPoolBuilder.disposable(builder.allowDisposableConnection);
        connectionPoolBuilder.borrowValidation(builder.borrowValidation);
        connectionPoolBuilder.returnValidation(builder.returnValidation);
        connectionPool = connectionPoolBuilder.build();

        this.failover = builder.failover;
        this.retryCount = builder.retryCount;

        this.initialServers = builder.servers;

        if (failover && healthMonitorIntervalInSecs > 0) {
            healthMonitorTask = new HealthMonitorTask();
            scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
            scheduledFuture = scheduledExecutor.scheduleWithFixedDelay(healthMonitorTask, healthMonitorIntervalInSecs, healthMonitorIntervalInSecs, TimeUnit.SECONDS);
        } else {
            healthMonitorTask = null;
            scheduledExecutor = null;
            scheduledFuture = null;
        }

        this.zkClient = builder.zkClient;
        this.zkListener = new ServerListBarrierListener(this, initialServers);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        final Processor processor = transport.getProcessor();
        if (!(processor instanceof FilterChain)) {
            throw new IllegalStateException("transport's processor has to be a FilterChain");
        }
        if (initialServers != null) {
            for (SocketAddress address : initialServers) {
                addServer(address);
            }
            roundRobinStore.shuffle();
        }
        if (zkClient != null) {
            // need to initialize the remote server with local initalServers if the remote server data is empty?
            // currently, do nothing
            zooKeeperServerListPath = zkClient.registerBarrier(thriftClientName, zkListener, null);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdown();
        }
        if (initialServers != null) {
            initialServers.clear();
        }
        roundRobinStore.clear();
        if (connectionPool != null) {
            connectionPool.destroy();
        }
        if (zkClient != null) {
            zkClient.unregisterBarrier(thriftClientName);
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean addServer(final SocketAddress serverAddress) {
        return addServer(serverAddress, true);
    }

    @SuppressWarnings("unchecked")
    private boolean addServer(final SocketAddress serverAddress, boolean initial) {
        if (serverAddress == null) {
            return true;
        }
        if (connectionPool != null) {
            try {
                connectionPool.createAllMinObjects(serverAddress);
            } catch (Exception e) {
                if (logger.isLoggable(Level.SEVERE)) {
                    logger.log(Level.SEVERE, "failed to create min connections in the pool. address=" + serverAddress, e);
                }
                try {
                    connectionPool.destroy(serverAddress);
                } catch (Exception ignore) {
                }
                if (!initial) {
                    return false;
                }
            }
        }
        roundRobinStore.add(serverAddress);
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, "added the server successfully. address={0}", serverAddress);
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeServer(final SocketAddress serverAddress) {
        removeServer(serverAddress, true);
    }

    private void removeServer(final SocketAddress serverAddress, final boolean forcibly) {
        if (serverAddress == null) {
            return;
        }
        if (!forcibly) {
            if (healthMonitorTask != null && healthMonitorTask.failure(serverAddress) &&
                    !(retainLastServer && roundRobinStore.hasOnly(serverAddress))) {
                roundRobinStore.remove(serverAddress);
                if (logger.isLoggable(Level.INFO)) {
                    logger.log(Level.INFO, "removed the server successfully. address={0}", serverAddress);
                }
            }
        } else {
            roundRobinStore.remove(serverAddress);
            if (logger.isLoggable(Level.INFO)) {
                logger.log(Level.INFO, "removed the server successfully. address={0}", serverAddress);
            }
        }
        if (connectionPool != null) {
            try {
                connectionPool.destroy(serverAddress);
                if (logger.isLoggable(Level.INFO)) {
                    logger.log(Level.INFO, "removed the server in the pool successfully. address={0}", serverAddress);
                }
            } catch (Exception e) {
                if (logger.isLoggable(Level.WARNING)) {
                    logger.log(Level.WARNING, "failed to remove connections in the pool", e);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isInServerList(final SocketAddress serverAddress) {
        return roundRobinStore.hasValue(serverAddress);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isZooKeeperSupported() {
        return zkClient != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getZooKeeperServerListPath() {
        if (!isZooKeeperSupported()) {
            return null;
        }
        return zooKeeperServerListPath;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCurrentServerListFromZooKeeper() {
        if (!isZooKeeperSupported()) {
            return null;
        }
        final byte[] serverListBytes = zkClient.getData(zooKeeperServerListPath, null);
        if (serverListBytes == null) {
            return null;
        }
        final String serverListString;
        try {
            serverListString = new String(serverListBytes, ServerListBarrierListener.DEFAULT_SERVER_LIST_CHARSET);
        } catch (UnsupportedEncodingException e) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.log(Level.WARNING, "failed to decode the server list bytes");
            }
            return null;
        }
        return serverListString;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean setCurrentServerListOfZooKeeper(final String thriftServerList) {
        if (!isZooKeeperSupported()) {
            return false;
        }
        if (thriftServerList == null) {
            return false;
        }
        final byte[] serverListBytes;
        try {
            serverListBytes = thriftServerList.getBytes(ServerListBarrierListener.DEFAULT_SERVER_LIST_CHARSET);
        } catch (UnsupportedEncodingException e) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.log(Level.WARNING, "failed to eecode the server list");
            }
            return false;
        }
        return zkClient.setData(zooKeeperServerListPath, serverListBytes, -1) != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addZooKeeperListener(final BarrierListener listener) {
        if (!isZooKeeperSupported()) {
            return;
        }
        zkListener.addCustomListener(listener);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeZooKeeperListener(final BarrierListener listener) {
        if (!isZooKeeperSupported()) {
            return;
        }
        zkListener.removeCustomListener(listener);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return thriftClientName;
    }

    @Override
    public <U> U execute(final ThriftClientCallback<T, U> callback) throws Exception {
        for (int i = 0; i <= retryCount; i++) {
            T client;
            final SocketAddress address = roundRobinStore.get();
            try {
                client = connectionPool.borrowObject(address, connectTimeoutInMillis);
            } catch (PoolExhaustedException pee) {
                if (logger.isLoggable(Level.FINER)) {
                    logger.log(Level.FINER, "failed to get the client. address=" + address + ", timeout=" + connectTimeoutInMillis + "ms", pee);
                }
                continue;
            } catch (NoValidObjectException nvoe) {
                if (logger.isLoggable(Level.FINER)) {
                    logger.log(Level.FINER, "failed to get the client. address=" + address + ", timeout=" + connectTimeoutInMillis + "ms", nvoe);
                }
                removeServer(address, false);
                continue;
            } catch (TimeoutException te) {
                if (logger.isLoggable(Level.FINER)) {
                    logger.log(Level.FINER, "failed to get the client. address=" + address + ", timeout=" + connectTimeoutInMillis + "ms", te);
                }
                continue;
            } catch (InterruptedException ie) {
                if (logger.isLoggable(Level.FINER)) {
                    logger.log(Level.FINER, "failed to get the client. address=" + address + ", timeout=" + connectTimeoutInMillis + "ms", ie);
                }
                throw ie;
            }
            if (client == null) {
                continue;
            }
            final boolean isMaxRetryCountReached = (i == retryCount);
            final Level logLevel;
            if (isMaxRetryCountReached) {
                logLevel = Level.INFO;
            } else {
                logLevel = Level.FINER;
            }
            boolean systemException = false;
            try {
                return callback.call(client);
            } catch (TTimedoutException tte) {
                systemException = true;
                if (logger.isLoggable(logLevel)) {
                    logger.log(logLevel, "timed out. address=" + address + ", client=" + client + ", callback" + callback, tte);
                }
                try {
                    connectionPool.removeObject(address, client);
                } catch (Exception e) {
                    if (logger.isLoggable(logLevel)) {
                        logger.log(logLevel, "failed to remove the client. address=" + address + ", client=" + client, e);
                    }
                }
            } catch (TProtocolException tpe) {
                systemException = true;
                if (logger.isLoggable(logLevel)) {
                    logger.log(logLevel, "occurred a thrift protocol error. address=" + address + ", client=" + client + ", callback" + callback, tpe);
                }
                try {
                    connectionPool.removeObject(address, client);
                } catch (Exception e) {
                    if (logger.isLoggable(logLevel)) {
                        logger.log(logLevel, "failed to remove the client. address=" + address + ", client=" + client, e);
                    }
                }
            } catch (TTransportException tte) {
                systemException = true;
                if (logger.isLoggable(logLevel)) {
                    logger.log(logLevel, "occurred a thrift trasport error. address=" + address + ", client=" + client + ", callback" + callback, tte);
                }
                try {
                    connectionPool.removeObject(address, client);
                } catch (Exception e) {
                    if (logger.isLoggable(logLevel)) {
                        logger.log(logLevel, "failed to remove the client. address=" + address + ", client=" + client, e);
                    }
                }
            } finally {
                if (!systemException) {
                    try {
                        connectionPool.returnObject(address, client);
                    } catch (Exception e) {
                        if (logger.isLoggable(logLevel)) {
                            logger.log(logLevel, "failed to return the client. address=" + address + ", client=" + client, e);
                        }
                    }
                }
            }
        }
        throw new IOException("failed to get the valid client");
    }

    private boolean validateClient(final T client) {
        if (validationCheckMethodName == null) {
            return true;
        }
        if (client == null) {
            return false;
        }
        try {
            final Method m = client.getClass().getMethod(validationCheckMethodName);
            try {
                m.invoke(client);
            } catch (Throwable t) {
                if (logger.isLoggable(Level.WARNING)) {
                    logger.log(Level.WARNING, "the client is not valid. client=" + client, t);
                }
                return false;
            }
        } catch (Throwable ignore) {
            if (logger.isLoggable(Level.FINER)) {
                logger.log(Level.FINER, "the '" + validationCheckMethodName + "()' method has not been implemented.", ignore);
            }
        }
        return true;
    }

    private boolean validateConnection(final Connection connection) {
        if (connection == null) {
            return false;
        }
        final TGrizzlyClientTransport ttransport = TGrizzlyClientTransport.create(connection, responseTimeoutInMillis, writeTimeoutInMillis);
        final TProtocol protocol;
        if (thriftProtocol == ThriftProtocols.BINARY) {
            protocol = new TBinaryProtocol(ttransport);
        } else if (thriftProtocol == ThriftProtocols.COMPACT) {
            protocol = new TCompactProtocol(ttransport);
        } else {
            protocol = new TBinaryProtocol(ttransport);
        }
        final T client = clientFactory.getClient(protocol);
        final boolean result = validateClient(client);
        ttransport.close();
        return result;
    }

    private class HealthMonitorTask implements Runnable {

        private final Map<SocketAddress, Boolean> failures = new ConcurrentHashMap<SocketAddress, Boolean>();
        private final Map<SocketAddress, Boolean> revivals = new ConcurrentHashMap<SocketAddress, Boolean>();
        private final AtomicBoolean running = new AtomicBoolean();

        public boolean failure(final SocketAddress address) {
            if (address == null) {
                return true;
            }
            if (failures.get(address) == null && revivals.get(address) == null) {
                failures.put(address, Boolean.TRUE);
                return true;
            } else {
                return false;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void run() {
            if (transport == null) {
                throw new IllegalStateException("transport must not be null");
            }
            if (!running.compareAndSet(false, true)) {
                return;
            }
            try {
                revivals.clear();
                final Set<SocketAddress> failuresSet = failures.keySet();
                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, "try to check the failures in health monitor. failed list hint={0}, interval={1}secs", new Object[]{failuresSet, healthMonitorIntervalInSecs});
                } else if (logger.isLoggable(Level.INFO) && !failuresSet.isEmpty()) {
                    logger.log(Level.INFO, "try to check the failures in health monitor. failed list hint={0}, interval={1}secs", new Object[]{failuresSet, healthMonitorIntervalInSecs});
                }
                for (SocketAddress failure : failuresSet) {
                    try {
                        // get the temporary connection
                        final ConnectorHandler<SocketAddress> connectorHandler = TCPNIOConnectorHandler.builder(transport).setReuseAddress(true).build();
                        Future<Connection> future = connectorHandler.connect(failure);
                        final Connection<SocketAddress> connection;
                        try {
                            if (connectTimeoutInMillis < 0) {
                                connection = future.get();
                            } else {
                                connection = future.get(connectTimeoutInMillis, TimeUnit.MILLISECONDS);
                            }
                        } catch (InterruptedException ie) {
                            if (!future.cancel(false) && future.isDone()) {
                                final Connection c = future.get();
                                if (c != null && c.isOpen()) {
                                    c.closeSilently();
                                }
                            }
                            if (logger.isLoggable(Level.SEVERE)) {
                                logger.log(Level.SEVERE, "failed to get the connection in health monitor. address=" + failure, ie);
                            }
                            continue;
                        } catch (ExecutionException ee) {
                            if (!future.cancel(false) && future.isDone()) {
                                final Connection c = future.get();
                                if (c != null && c.isOpen()) {
                                    c.closeSilently();
                                }
                            }
                            if (logger.isLoggable(Level.SEVERE)) {
                                logger.log(Level.SEVERE, "failed to get the connection in health monitor. address=" + failure, ee);
                            }
                            continue;
                        } catch (TimeoutException te) {
                            if (!future.cancel(false) && future.isDone()) {
                                final Connection c = future.get();
                                if (c != null && c.isOpen()) {
                                    c.closeSilently();
                                }
                            }
                            if (logger.isLoggable(Level.SEVERE)) {
                                logger.log(Level.SEVERE, "failed to get the connection in health monitor. address=" + failure, te);
                            }
                            continue;
                        }
                        if (validateConnection(connection)) {
                            failures.remove(failure);
                            revivals.put(failure, Boolean.TRUE);
                        }
                        connection.closeSilently();
                    } catch (Throwable t) {
                        if (logger.isLoggable(Level.SEVERE)) {
                            logger.log(Level.SEVERE, "unexpected exception thrown", t);
                        }
                    }
                }
                final Set<SocketAddress> revivalsSet = revivals.keySet();
                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, "try to restore revivals in health monitor. revival list hint={0}, interval={1}secs", new Object[]{revivalsSet, healthMonitorIntervalInSecs});
                } else if (logger.isLoggable(Level.INFO) && !revivalsSet.isEmpty()) {
                    logger.log(Level.INFO, "try to restore revivals in health monitor. revival list hint={0}, interval={1}secs", new Object[]{revivalsSet, healthMonitorIntervalInSecs});
                }
                for (SocketAddress revival : revivalsSet) {
                    if (!addServer(revival, false)) {
                        if (logger.isLoggable(Level.WARNING)) {
                            logger.log(Level.WARNING, "the revival was failed again in health monitor. revival={0}", revival);
                        }
                        failures.put(revival, Boolean.TRUE);
                    }
                }
            } finally {
                running.set(false);
            }
        }
    }

    public static class Builder<T extends TServiceClient> implements ThriftClientBuilder {

        private final String thriftClientName;
        private final GrizzlyThriftClientManager manager;
        private final TCPNIOTransport transport;
        private final TServiceClientFactory<T> clientFactory;

        private Set<SocketAddress> servers;
        private long connectTimeoutInMillis = 5000; // 5secs
        private long writeTimeoutInMillis = 5000; // 5secs
        private long responseTimeoutInMillis = 10000; // 10secs

        private long healthMonitorIntervalInSecs = 60; // 1 min
        private boolean failover = true;
        private int retryCount = 1;
        private ThriftProtocols thriftProtocol = ThriftProtocols.BINARY;
        private TransferProtocols transferProtocol = TransferProtocols.BASIC;
        private String validationCheckMethodName = null;

        // connection pool config
        private int minConnectionPerServer = 5;
        private int maxConnectionPerServer = Integer.MAX_VALUE;
        private long keepAliveTimeoutInSecs = 30 * 60; // 30 min
        private boolean allowDisposableConnection = false;
        private boolean borrowValidation = false;
        private boolean returnValidation = false;
        private boolean retainLastServer = false;

        private final ZKClient zkClient;
        private int maxThriftFrameLength;
        private String httpUriPath = "/";

        public Builder(final String thriftClientName, final GrizzlyThriftClientManager manager, final TCPNIOTransport transport, final TServiceClientFactory<T> clientFactory) {
            this.thriftClientName = thriftClientName;
            this.manager = manager;
            this.transport = transport;
            this.clientFactory = clientFactory;
            this.zkClient = manager.getZkClient();
            this.maxThriftFrameLength = manager.getMaxThriftFrameLength();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public GrizzlyThriftClient<T> build() {
            final GrizzlyThriftClient<T> thriftClient = new GrizzlyThriftClient<T>(this);
            thriftClient.start();
            if (!manager.addThriftClient(thriftClient)) {
                thriftClient.stop();
                throw new IllegalStateException("failed to add the thrift client because the ThriftClientManager already stopped or the same thrift client name existed");
            }
            return thriftClient;
        }

        /**
         * Set global connect-timeout
         * <p/>
         * If the given param is negative, the timeout is infite.
         * Default is 5000.
         *
         * @param connectTimeoutInMillis connect-timeout in milli-seconds
         * @return this builder
         */
        public Builder<T> connectTimeoutInMillis(final long connectTimeoutInMillis) {
            this.connectTimeoutInMillis = connectTimeoutInMillis;
            return this;
        }

        /**
         * Set global write-timeout
         * <p/>
         * If the given param is negative, the timeout is infite.
         * Default is 5000.
         *
         * @param writeTimeoutInMillis write-timeout in milli-seconds
         * @return this builder
         */
        public Builder<T> writeTimeoutInMillis(final long writeTimeoutInMillis) {
            this.writeTimeoutInMillis = writeTimeoutInMillis;
            return this;
        }

        /**
         * Set global response-timeout
         * <p/>
         * If the given param is negative, the timeout is infite.
         * Default is 10000.
         *
         * @param responseTimeoutInMillis response-timeout in milli-seconds
         * @return this builder
         */
        public Builder<T> responseTimeoutInMillis(final long responseTimeoutInMillis) {
            this.responseTimeoutInMillis = responseTimeoutInMillis;
            return this;
        }

        /**
         * Set connection pool's min
         * <p/>
         * Default is 5.
         *
         * @param minConnectionPerServer connection pool's min
         * @return this builder
         * @see BaseObjectPool.Builder#min(int)
         */
        public Builder<T> minConnectionPerServer(final int minConnectionPerServer) {
            this.minConnectionPerServer = minConnectionPerServer;
            return this;
        }

        /**
         * Set connection pool's max
         * <p/>
         * Default is {@link Integer#MAX_VALUE}
         *
         * @param maxConnectionPerServer connection pool's max
         * @return this builder
         * @see BaseObjectPool.Builder#max(int)
         */
        public Builder<T> maxConnectionPerServer(final int maxConnectionPerServer) {
            this.maxConnectionPerServer = maxConnectionPerServer;
            return this;
        }

        /**
         * Set connection pool's KeepAliveTimeout
         * <p/>
         * Default is 1800.
         *
         * @param keepAliveTimeoutInSecs connection pool's KeepAliveTimeout in seconds
         * @return this builder
         * @see BaseObjectPool.Builder#keepAliveTimeoutInSecs(long)
         */
        public Builder<T> keepAliveTimeoutInSecs(final long keepAliveTimeoutInSecs) {
            this.keepAliveTimeoutInSecs = keepAliveTimeoutInSecs;
            return this;
        }

        /**
         * Set health monitor's interval
         * <p/>
         * This thrift client will schedule HealthMonitorTask with this interval.
         * HealthMonitorTask will check the failure servers periodically and detect the revived server.
         * If the given parameter is negative, this thrift client never schedules HealthMonitorTask
         * so this behavior is similar to seting {@code failover} to be false.
         * Default is 60.
         *
         * @param healthMonitorIntervalInSecs interval in seconds
         * @return this builder
         */
        public Builder<T> healthMonitorIntervalInSecs(final long healthMonitorIntervalInSecs) {
            this.healthMonitorIntervalInSecs = healthMonitorIntervalInSecs;
            return this;
        }

        /**
         * Allow or disallow disposable connections
         * <p/>
         * Default is false.
         *
         * @param allowDisposableConnection true if this thrift client allows disposable connections
         * @return this builder
         */
        public Builder<T> allowDisposableConnection(final boolean allowDisposableConnection) {
            this.allowDisposableConnection = allowDisposableConnection;
            return this;
        }

        /**
         * Enable or disable the connection validation when the connection is borrowed from the connection pool
         * <p/>
         * Default is false.
         *
         * @param borrowValidation true if this thrift client should make sure the borrowed connection is valid
         * @return this builder
         */
        public Builder<T> borrowValidation(final boolean borrowValidation) {
            this.borrowValidation = borrowValidation;
            return this;
        }

        /**
         * Enable or disable the connection validation when the connection is returned to the connection pool
         * <p/>
         * Default is false.
         *
         * @param returnValidation true if this thrift client should make sure the returned connection is valid
         * @return this builder
         */
        public Builder<T> returnValidation(final boolean returnValidation) {
            this.returnValidation = returnValidation;
            return this;
        }

        /**
         * Enable or disable the keeping a server in the the round-robin list when the only one server is remained in the list.
         * <p/>
         * Default is false
         *
         * @param retainLastServer true if this thrift client should make sure the retaining one server in the round-robin list.
         * @return this builder
         */
        public Builder<T> retainLastServer(final boolean retainLastServer) {
            this.retainLastServer = retainLastServer;
            return this;
        }

        /**
         * Set initial servers
         *
         * @param servers server set
         * @return this builder
         */
        public Builder<T> servers(final Set<SocketAddress> servers) {
            this.servers = new HashSet<SocketAddress>(servers);
            return this;
        }

        /**
         * Enable or disable failover/failback
         * <p/>
         * Default is true.
         *
         * @param failover true if this thrift client should support failover/failback when the server is failed or revived
         * @return this builder
         */
        public Builder<T> failover(final boolean failover) {
            this.failover = failover;
            return this;
        }

        /**
         * Set retry count for connection or sending
         * <p/>
         * Default is 1.
         *
         * @param retryCount the count for retrials
         * @return this builder
         */
        public Builder<T> retryCount(final int retryCount) {
            this.retryCount = retryCount;
            return this;
        }

        public Builder<T> thriftProtocol(final ThriftProtocols thriftProtocol) {
            this.thriftProtocol = thriftProtocol;
            return this;
        }

        /**
         * Set the max length of thrift frame
         *
         * @param maxThriftFrameLength max frame length
         * @return this builder
         */
        public Builder maxThriftFrameLength(final int maxThriftFrameLength) {
            this.maxThriftFrameLength = maxThriftFrameLength;
            return this;
        }

        public Builder<T> httpUriPath(final String httpUriPath) {
            if (httpUriPath == null || httpUriPath.isEmpty()) {
                return this;
            }
            this.transferProtocol = TransferProtocols.HTTP;
            this.httpUriPath = httpUriPath;
            return this;
        }

        public Builder<T> validationCheckMethodName(final String validationCheckMethodName) {
            if (validationCheckMethodName != null) {
                this.validationCheckMethodName = validationCheckMethodName;
            }
            return this;
        }
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(256);
        sb.append("GrizzlyThriftClient{");
        sb.append("thriftClientName='").append(thriftClientName).append('\'');
        sb.append(", transport=").append(transport);
        sb.append(", connectTimeoutInMillis=").append(connectTimeoutInMillis);
        sb.append(", writeTimeoutInMillis=").append(writeTimeoutInMillis);
        sb.append(", responseTimeoutInMillis=").append(responseTimeoutInMillis);
        sb.append(", validationCheckMethodName='").append(validationCheckMethodName).append('\'');
        sb.append(", connectionPool=").append(connectionPool);
        sb.append(", initialServers=").append(initialServers);
        sb.append(", healthMonitorIntervalInSecs=").append(healthMonitorIntervalInSecs);
        sb.append(", healthMonitorTask=").append(healthMonitorTask);
        sb.append(", retainLastServer=").append(retainLastServer);
        sb.append(", failover=").append(failover);
        sb.append(", retryCount=").append(retryCount);
        sb.append(", thriftProtocol=").append(thriftProtocol);
        sb.append(", clientFactory=").append(clientFactory);
        sb.append(", zooKeeperServerListPath='").append(zooKeeperServerListPath).append('\'');
        sb.append(", transferProtocol=").append(transferProtocol);
        sb.append(", maxThriftFrameLength=").append(maxThriftFrameLength);
        sb.append(", httpUriPath='").append(httpUriPath).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
