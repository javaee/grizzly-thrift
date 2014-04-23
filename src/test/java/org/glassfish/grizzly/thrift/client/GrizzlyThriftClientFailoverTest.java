/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2014 Oracle and/or its affiliates. All rights reserved.
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

import org.apache.thrift.TProcessor;
import org.glassfish.grizzly.filterchain.FilterChainBuilder;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.filterchain.TransportFilter;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;
import org.glassfish.grizzly.thrift.CalculatorHandler;
import org.glassfish.grizzly.thrift.ThriftFrameFilter;
import org.glassfish.grizzly.thrift.ThriftServerFilter;
import org.junit.Assert;
import org.junit.Test;
import tutorial.Calculator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * GrizzlyThriftClient's failover test
 *
 * @author Flutia
 */
public class GrizzlyThriftClientFailoverTest {

    private static final int PORT = 7791;
    private static final int INVALID_PORT1 = 7800;
    private static final int FAILOVER_PORT = 7792;

    @Test
    public void testServerRemains() throws Exception {
        // create manager
        final GrizzlyThriftClientManager manager = new GrizzlyThriftClientManager.Builder().build();

        // create builder
        final GrizzlyThriftClient.Builder<Calculator.Client> builder = manager.createThriftClientBuilder("Calculator", new Calculator.Client.Factory());
        builder.connectTimeoutInMillis(1000L);
        builder.responseTimeoutInMillis(500L);
        builder.minConnectionPerServer(10);
        builder.retainLastServer(true);
        // create client
        final ThriftClient<Calculator.Client> calculatorThriftClient = builder.build();
        calculatorThriftClient.addServer(new InetSocketAddress("localhost", PORT));

        final ThriftClientCallback<Calculator.Client, Integer> clientCallback = new ThriftClientCallback<Calculator.Client, Integer>() {
            @Override
            public Integer call(Calculator.Client client) throws Exception {
                return client.add(1, 2);
            }
        };

        TCPNIOTransport thriftServer = null;
        try {
            try {
                calculatorThriftClient.execute(clientCallback);
                Assert.fail();
            } catch (IOException e) {
                assertTrue(e.getMessage().contains("failed to get the valid client"));
            }

            // start server
            Calculator.Processor processor = new Calculator.Processor(new CalculatorHandler());
            thriftServer = createThriftServer(PORT, new ThriftServerFilter(processor));
            Thread.sleep(100);

            // success
            Integer result = calculatorThriftClient.execute(clientCallback);
            assertTrue(result == 3);

        } finally {
            manager.removeThriftClient("Calculator");
            manager.shutdown();

            if (thriftServer != null) {
                thriftServer.shutdown();
            }
        }
    }

    @Test
    public void testFailover() throws Exception {
        // create manager
        final GrizzlyThriftClientManager manager = new GrizzlyThriftClientManager.Builder().build();

        // create builder
        final GrizzlyThriftClient.Builder<Calculator.Client> builder = manager.createThriftClientBuilder("Calculator", new Calculator.Client.Factory());
        builder.connectTimeoutInMillis(1000L);
        builder.responseTimeoutInMillis(500L);
        builder.retainLastServer(true);
        builder.minConnectionPerServer(10);
        // create client
        final ThriftClient<Calculator.Client> calculatorThriftClient = builder.build();
        calculatorThriftClient.addServer(new InetSocketAddress("localhost", PORT));

        final ThriftClientCallback<Calculator.Client, Integer> clientCallback = new ThriftClientCallback<Calculator.Client, Integer>() {
            @Override
            public Integer call(Calculator.Client client) throws Exception {
                return client.add(1, 2);
            }
        };

        // start server
        TCPNIOTransport firstServer = createThriftServer(PORT, new ThriftServerFilter(new Calculator.Processor(new CalculatorHandler())));
        Thread.sleep(100);

        try {
            // success
            Integer result = calculatorThriftClient.execute(clientCallback);
            assertTrue(result == 3);

            // must fail
            firstServer.shutdown();
            try {
                calculatorThriftClient.execute(clientCallback);
                Assert.fail();
            } catch(IOException e) {
                assertTrue(e.getMessage().contains("failed to get the valid client"));
            }

            // start another server
            TCPNIOTransport secondServer = createThriftServer(PORT, new ThriftServerFilter(new Calculator.Processor(new CalculatorHandler())));
            Thread.sleep(100);

            result = calculatorThriftClient.execute(clientCallback);
            assertTrue(result == 3);

            secondServer.shutdown();

        } finally {
            manager.removeThriftClient("Calculator");
            manager.shutdown();
        }
    }

    @Test
    public void testFailoverWithMultiClient() throws Exception {
        // create manager
        final GrizzlyThriftClientManager manager = new GrizzlyThriftClientManager.Builder().build();

        // create builder
        final GrizzlyThriftClient.Builder<Calculator.Client> builder = manager.createThriftClientBuilder("Calculator", new Calculator.Client.Factory());
        // builder.validationCheckMethodName("ping");
        // builder.borrowValidation(true);
        builder.connectTimeoutInMillis(1000L);
        builder.responseTimeoutInMillis(500L);
        builder.minConnectionPerServer(10);
        builder.retainLastServer(true);
        // create client
        final ThriftClient<Calculator.Client> calculatorThriftClient = builder.build();
        calculatorThriftClient.addServer(new InetSocketAddress("localhost", PORT));
        calculatorThriftClient.addServer(new InetSocketAddress("localhost", FAILOVER_PORT));
        calculatorThriftClient.addServer(new InetSocketAddress("localhost", INVALID_PORT1));

        // create server
        final TCPNIOTransport thriftServer = createThriftServer(PORT, new ThriftServerFilter(new Calculator.Processor(new CalculatorHandler())));
        final TCPNIOTransport thriftServerForFailover = createThriftServer(FAILOVER_PORT, new ThriftServerFilter(new Calculator.Processor(new CalculatorHandler())));

        final int clientsNum = Runtime.getRuntime().availableProcessors() * 4;
        final Integer executionNum = 20;
        final CountDownLatch allFinished = new CountDownLatch(clientsNum);
        final ThriftClientCallback<Calculator.Client, Integer> clientCallback = new ThriftClientCallback<Calculator.Client, Integer>() {
            @Override
            public Integer call(Calculator.Client client) throws Exception {
                return client.add(1, 2);
            }
        };

        final AtomicInteger counter = new AtomicInteger(0);
        try {
            for (int i = 0; i < clientsNum; i++) {
                new Thread() {
                    public void run() {
                        try {
                            for (int j = 0; j < executionNum; j++) {
                                try {
                                    Integer result = calculatorThriftClient.execute(clientCallback);
                                    assertTrue(result == 3);
                                    counter.incrementAndGet();

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
            assertEquals(clientsNum * executionNum, counter.get());

        } finally {
            manager.removeThriftClient("Calculator");
            manager.shutdown();

            if (thriftServer != null) {
                thriftServer.shutdown();
            }
            if (thriftServerForFailover != null) {
                thriftServerForFailover.shutdown();
            }
        }
    }

    @Test
    public void testFailoverWithMultiClientAndServerClose() throws Exception {
        // create manager
        final GrizzlyThriftClientManager manager = new GrizzlyThriftClientManager.Builder().build();

        // create builder
        final GrizzlyThriftClient.Builder<Calculator.Client> builder = manager.createThriftClientBuilder("Calculator", new Calculator.Client.Factory());
        // builder.validationCheckMethodName("ping");
        // builder.borrowValidation(true);
        builder.connectTimeoutInMillis(1000L);
        builder.responseTimeoutInMillis(500L);
        builder.minConnectionPerServer(10);
        builder.retainLastServer(true);
        // create client
        final ThriftClient<Calculator.Client> calculatorThriftClient = builder.build();
        calculatorThriftClient.addServer(new InetSocketAddress("localhost", PORT));
        calculatorThriftClient.addServer(new InetSocketAddress("localhost", FAILOVER_PORT));

        // create server
        TestThriftServerFilter filter = new TestThriftServerFilter(new Calculator.Processor(new CalculatorHandler()));
        final TCPNIOTransport thriftServer = createThriftServer(PORT, filter);
        final TCPNIOTransport thriftServerForFailover = createThriftServer(FAILOVER_PORT, new ThriftServerFilter(new Calculator.Processor(new CalculatorHandler())));

        final int clientsNum = Runtime.getRuntime().availableProcessors() * 4;
        final Integer executionNum = 20;
        final CountDownLatch allFinished = new CountDownLatch(clientsNum);
        final ThriftClientCallback<Calculator.Client, Integer> clientCallback = new ThriftClientCallback<Calculator.Client, Integer>() {
            @Override
            public Integer call(Calculator.Client client) throws Exception {
                return client.add(1, 2);
            }
        };

        final AtomicInteger counter = new AtomicInteger(0);
        try {
            for (int i = 0; i < clientsNum; i++) {
                if (counter.get() == 1) {
                    filter.setDisconnectFlag();
                }

                new Thread() {
                    public void run() {
                        try {
                            for (int j = 0; j < executionNum; j++) {
                                try {
                                    Integer result = calculatorThriftClient.execute(clientCallback);
                                    assertTrue(result == 3);
                                    counter.incrementAndGet();

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
            assertEquals(clientsNum * executionNum, counter.get());

        } finally {
            manager.removeThriftClient("Calculator");
            manager.shutdown();

            if (thriftServer != null) {
                thriftServer.shutdown();
            }
            if (thriftServerForFailover != null) {
                thriftServerForFailover.shutdown();
            }
        }
    }

    private static TCPNIOTransport createThriftServer(final int port, final ThriftServerFilter filter) throws IOException {
        final FilterChainBuilder filterChainBuilder = FilterChainBuilder.stateless();
        filterChainBuilder.add(new TransportFilter());
        filterChainBuilder.add(new ThriftFrameFilter());
        filterChainBuilder.add(filter);
        final TCPNIOTransport transport = TCPNIOTransportBuilder.newInstance().build();
        transport.setProcessor(filterChainBuilder.build());
        transport.bind(port);
        transport.start();
        return transport;
    }

    private static class TestThriftServerFilter extends ThriftServerFilter {
        private volatile boolean closeFlag;

        public void setDisconnectFlag() {
            closeFlag = true;
        }

        public TestThriftServerFilter(TProcessor processor) {
            super(processor);
        }

        @Override
        public NextAction handleRead(FilterChainContext ctx) throws IOException {
            if (closeFlag) {
                ctx.getConnection().closeSilently();
                closeFlag = false;
                return ctx.getStopAction();
            }

            return super.handleRead(ctx);
        }
    }
}
