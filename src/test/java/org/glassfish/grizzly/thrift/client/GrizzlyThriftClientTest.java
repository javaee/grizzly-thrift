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

import org.glassfish.grizzly.filterchain.FilterChainBuilder;
import org.glassfish.grizzly.filterchain.TransportFilter;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;
import org.glassfish.grizzly.thrift.CalculatorHandler;
import org.glassfish.grizzly.thrift.ThriftFrameFilter;
import org.glassfish.grizzly.thrift.ThriftServerFilter;
import org.glassfish.grizzly.thrift.http.ThriftHttpHandler;
import org.junit.Assert;
import org.junit.Test;
import shared.SharedStruct;
import tutorial.Calculator;
import tutorial.Operation;
import tutorial.Work;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * GrizzlyThriftClient's test
 *
 * @author Bongjae Chang
 */
public class GrizzlyThriftClientTest {

    private static final int PORT = 7791;
    private static final int FAILOVER_PORT = 7792;
    private static final int FAILED_SERVICE_PORT = 7793;
    private static final int FAILED_SERVICE_PORT_2 = 7794;

    @Test
    public void testBasic() throws Exception {
        @SuppressWarnings("unchecked")
        final TCPNIOTransport transport = createThriftServer(PORT, new Calculator.Processor(new CalculatorHandler()));

        // create manager
        final GrizzlyThriftClientManager manager = new GrizzlyThriftClientManager.Builder().build();

        // create builder
        final GrizzlyThriftClient.Builder<Calculator.Client> builder = manager.createThriftClientBuilder("Calculator", new Calculator.Client.Factory());
        final Set<SocketAddress> initServerSet = new HashSet<SocketAddress>();
        initServerSet.add(new InetSocketAddress("localhost", PORT));
        builder.servers(initServerSet);
        builder.validationCheckMethodName("ping");
        builder.connectTimeoutInMillis(1000L);

        // create client
        final ThriftClient<Calculator.Client> calculatorThriftClient = builder.build();

        try {
            // execute
            perform(calculatorThriftClient);
        } finally {
            // release
            manager.removeThriftClient("Calculator");
            manager.shutdown();

            transport.shutdownNow();
        }
    }

    @Test
    public void testOverHttp() throws Exception {
        final String uriPath = "/httptest1";
        @SuppressWarnings("unchecked")
        final HttpServer httpServer = createThriftHttpServer(PORT, new Calculator.Processor(new CalculatorHandler()), uriPath);

        // create manager
        final GrizzlyThriftClientManager manager = new GrizzlyThriftClientManager.Builder().build();

        // create builder
        final GrizzlyThriftClient.Builder<Calculator.Client> builder = manager.createThriftClientBuilder("Calculator", new Calculator.Client.Factory());
        final Set<SocketAddress> initServerSet = new HashSet<SocketAddress>();
        initServerSet.add(new InetSocketAddress("localhost", PORT));
        builder.servers(initServerSet);
        builder.validationCheckMethodName("ping");
        builder.connectTimeoutInMillis(1000L);
        builder.httpUriPath(uriPath);

        // create client
        final ThriftClient<Calculator.Client> calculatorThriftClient = builder.build();

        try {
            // execute
            perform(calculatorThriftClient);
        } finally {
            // release
            manager.removeThriftClient("Calculator");
            manager.shutdown();

            httpServer.shutdownNow();
        }
    }

    @Test
    public void testFailover() throws Exception {
        @SuppressWarnings("unchecked")
        final TCPNIOTransport thriftServer = createThriftServer(PORT, new Calculator.Processor(new CalculatorHandler()));
        @SuppressWarnings("unchecked")
        final TCPNIOTransport thriftServerForFailover = createThriftServer(FAILOVER_PORT, new Calculator.Processor(new CalculatorHandler()));
        @SuppressWarnings("unchecked")
        final TCPNIOTransport thriftServerForFailedServicePort = createThriftServer(FAILED_SERVICE_PORT, new Calculator.Processor(new CalculatorHandler() {
            @Override
            public void ping() {
                throw new IllegalStateException("I am always failed");
            }
        }));
        // create manager
        final GrizzlyThriftClientManager manager = new GrizzlyThriftClientManager.Builder().build();

        // create builder
        final GrizzlyThriftClient.Builder<Calculator.Client> builder = manager.createThriftClientBuilder("Calculator", new Calculator.Client.Factory());
        builder.validationCheckMethodName("ping");
        builder.borrowValidation(true);
        builder.connectTimeoutInMillis(1000L);
        builder.responseTimeoutInMillis(500L);

        // create client
        final ThriftClient<Calculator.Client> calculatorThriftClient = builder.build();

        final InetSocketAddress validUri1 = new InetSocketAddress("localhost", PORT);
        final InetSocketAddress invalidUri1 = new InetSocketAddress("127.0.0.1", FAILED_SERVICE_PORT);
        final InetSocketAddress validUri2 = new InetSocketAddress("127.0.0.1", FAILOVER_PORT);
        final InetSocketAddress invalidUri2 = new InetSocketAddress("localhost", FAILED_SERVICE_PORT_2);
        calculatorThriftClient.addServer(validUri1);
        calculatorThriftClient.addServer(invalidUri1);
        calculatorThriftClient.addServer(invalidUri2);
        calculatorThriftClient.addServer(validUri2);

        try {
            Assert.assertTrue(calculatorThriftClient.isInServerList(validUri1));
            Assert.assertTrue(calculatorThriftClient.isInServerList(invalidUri1));
            Assert.assertTrue(calculatorThriftClient.isInServerList(invalidUri2));
            Assert.assertTrue(calculatorThriftClient.isInServerList(validUri2));
            // 1, validUri1(valid) -->
            // 2, invalidUri1(failed) -> invalidUri1(removed) -> validUri2(valid) -->
            // 3. validUri1(valid) -->
            // 4. invalidUri2(failed) --> invalidUri2(removed) -> validUri2(valid) -->
            // 5. validUri1(valid)
            // 6. validUri2(valid)
            // ...
            for (int i = 0; i < 10; i++) {
                // execute
                Integer result = calculatorThriftClient.execute(new ThriftClientCallback<Calculator.Client, Integer>() {
                    @Override
                    public Integer call(Calculator.Client client) throws Exception {
                        return client.add(1, 2);
                    }
                });
                assertTrue(result == 3);
            }

            Assert.assertTrue(!calculatorThriftClient.isInServerList(invalidUri1));
            Assert.assertTrue(!calculatorThriftClient.isInServerList(invalidUri2));
        } finally {
            // release
            manager.removeThriftClient("Calculator");
            manager.shutdown();

            thriftServer.shutdownNow();
            thriftServerForFailover.shutdownNow();
            thriftServerForFailedServicePort.shutdownNow();
        }
    }

    @Test
    public void testMultipleClients() throws Exception {
        @SuppressWarnings("unchecked")
        final TCPNIOTransport thriftServer = createThriftServer(PORT, new Calculator.Processor(new CalculatorHandler()));

        // create manager
        final GrizzlyThriftClientManager manager = new GrizzlyThriftClientManager.Builder().build();

        // create builder
        final GrizzlyThriftClient.Builder<Calculator.Client> builder = manager.createThriftClientBuilder("Calculator", new Calculator.Client.Factory());
        final Set<SocketAddress> initServerSet = new HashSet<SocketAddress>();
        initServerSet.add(new InetSocketAddress("localhost", PORT));
        initServerSet.add(new InetSocketAddress("127.0.0.1", PORT));
        builder.servers(initServerSet);
        builder.connectTimeoutInMillis(1000L);
        builder.minConnectionPerServer(10);

        // create client
        final ThriftClient<Calculator.Client> calculatorThriftClient = builder.build();

        final int clientsNum = Runtime.getRuntime().availableProcessors() * 4;
        final Integer executionNum = 20;
        final CountDownLatch allFinished = new CountDownLatch(clientsNum);
        try {
            for (int i = 0; i < clientsNum; i++) {
                new Thread() {
                    public void run() {
                        try {
                            for (int j = 0; j < executionNum; j++) {
                                try {
                                    perform(calculatorThriftClient);
                                } catch (Exception t) {

                                    t.printStackTrace();
                                    Assert.fail();
                                }
                            }
                        } finally {
                            allFinished.countDown();
                        }
                    }
                }.start();
            }
            allFinished.await();
        } finally {
            // release
            manager.removeThriftClient("Calculator");
            manager.shutdown();

            thriftServer.shutdownNow();
        }
    }

    @Test
    public void testHealthMonitor() throws Exception {
        @SuppressWarnings("unchecked")
        final TCPNIOTransport thriftServer = createThriftServer(PORT, new Calculator.Processor(new CalculatorHandler()));

        // create manager
        final GrizzlyThriftClientManager manager = new GrizzlyThriftClientManager.Builder().build();

        // create builder
        final GrizzlyThriftClient.Builder<Calculator.Client> builder = manager.createThriftClientBuilder("Calculator", new Calculator.Client.Factory());
        final Set<SocketAddress> initServerSet = new HashSet<SocketAddress>();
        final SocketAddress address = new InetSocketAddress("localhost", PORT);
        initServerSet.add(address);
        builder.servers(initServerSet);
        builder.connectTimeoutInMillis(1000L);
        builder.responseTimeoutInMillis(500L);
        builder.healthMonitorIntervalInSecs(1);

        // create client
        final ThriftClient<Calculator.Client> calculatorThriftClient = builder.build();

        // success
        Integer result = calculatorThriftClient.execute(new ThriftClientCallback<Calculator.Client, Integer>() {
            @Override
            public Integer call(Calculator.Client client) throws Exception {
                return client.add(1, 2);
            }
        });
        assertTrue(result == 3);
        Assert.assertTrue(calculatorThriftClient.isInServerList(address));

        // stop the server
        thriftServer.shutdownNow();
        Thread.sleep(200);

        try {
            calculatorThriftClient.execute(new ThriftClientCallback<Calculator.Client, Integer>() {
                @Override
                public Integer call(Calculator.Client client) throws Exception {
                    return client.add(1, 2);
                }
            });
        } catch (Exception te) {
        }
        Assert.assertTrue(!calculatorThriftClient.isInServerList(address));

        // revival
        @SuppressWarnings("unchecked")
        final TCPNIOTransport thriftServer2 = createThriftServer(PORT, new Calculator.Processor(new CalculatorHandler()));

        // wait for recovery
        Thread.sleep(2000);

        // test again
        perform(calculatorThriftClient);
        Assert.assertTrue(calculatorThriftClient.isInServerList(address));

        // release
        manager.removeThriftClient("Calculator");
        manager.shutdown();

        thriftServer2.shutdownNow();
    }

    private static TCPNIOTransport createThriftServer(final int port, final Calculator.Processor tprocessor) throws IOException {
        final FilterChainBuilder filterChainBuilder = FilterChainBuilder.stateless();
        filterChainBuilder.add(new TransportFilter());
        filterChainBuilder.add(new ThriftFrameFilter());
        filterChainBuilder.add(new ThriftServerFilter(tprocessor));
        final TCPNIOTransport transport = TCPNIOTransportBuilder.newInstance().build();
        transport.setProcessor(filterChainBuilder.build());
        transport.bind(port);
        transport.start();
        return transport;
    }

    private static HttpServer createThriftHttpServer(final int port, final Calculator.Processor tprocessor, final String uriPath) throws IOException {
        final HttpServer server = new HttpServer();
        final NetworkListener listener = new NetworkListener("grizzly-thrift-http", NetworkListener.DEFAULT_NETWORK_HOST, port);
        server.addListener(listener);
        server.getServerConfiguration().addHttpHandler(new ThriftHttpHandler(tprocessor), uriPath);
        server.start();
        return server;
    }

    private static void perform(final ThriftClient<Calculator.Client> calculatorThriftClient) throws Exception {
        Integer result = calculatorThriftClient.execute(new ThriftClientCallback<Calculator.Client, Integer>() {
            @Override
            public Integer call(Calculator.Client client) throws Exception {
                return client.add(1, 2);
            }
        });
        assertTrue(result == 3);

        result = calculatorThriftClient.execute(new ThriftClientCallback<Calculator.Client, Integer>() {
            @Override
            public Integer call(Calculator.Client client) throws Exception {
                final Work work = new Work();
                work.op = Operation.DIVIDE;
                work.num1 = 25;
                work.num2 = 5;
                return client.calculate(1, work);
            }
        });
        assertTrue(result == 5);

        final SharedStruct log = calculatorThriftClient.execute(new ThriftClientCallback<Calculator.Client, SharedStruct>() {
            @Override
            public SharedStruct call(Calculator.Client client) throws Exception {
                return client.getStruct(1);
            }
        });
        assertTrue(log != null);
        assertEquals("5", log.value);
    }
}
