/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2011-2017 Oracle and/or its affiliates. All rights reserved.
 *
 * The contents of this file are subject to the terms of either the GNU
 * General Public License Version 2 only ("GPL") or the Common Development
 * and Distribution License("CDDL") (collectively, the "License").  You
 * may not use this file except in compliance with the License.  You can
 * obtain a copy of the License at
 * https://oss.oracle.com/licenses/CDDL+GPL-1.1
 * or LICENSE.txt.  See the License for the specific
 * language governing permissions and limitations under the License.
 *
 * When distributing the software, include this License Header Notice in each
 * file and include the License file at LICENSE.txt.
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

package org.glassfish.grizzly.thrift;

import org.apache.thrift.TServiceClient;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.Grizzly;
import org.glassfish.grizzly.attributes.Attribute;
import org.glassfish.grizzly.filterchain.BaseFilter;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.memory.Buffers;
import org.glassfish.grizzly.thrift.client.GrizzlyThriftClient;
import org.glassfish.grizzly.thrift.client.pool.ObjectPool;
import org.glassfish.grizzly.utils.DataStructures;
import org.glassfish.grizzly.utils.NullaryFunction;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ThriftClientFilter is a client-side filter for Thrift RPC processors.
 * <p>
 * Read-messages will be queued in LinkedBlockingQueue from which TGrizzlyClientTransport will read it.
 * <p>
 * Usages:
 * <pre>
 * {@code
 * final FilterChainBuilder clientFilterChainBuilder = FilterChainBuilder.stateless();
 * clientFilterChainBuilder.add(new TransportFilter()).add(new ThriftFrameFilter()).add(new ThriftClientFilter());
 * <p>
 * final TCPNIOTransport transport = TCPNIOTransportBuilder.newInstance().build();
 * transport.setProcessor(clientFilterChainBuilder.build());
 * transport.start();
 * Future<Connection> future = transport.connect(ip, port);
 * final Connection connection = future.get(10, TimeUnit.SECONDS);
 * <p>
 * final TTransport ttransport = TGrizzlyClientTransport.create(connection);
 * final TProtocol tprotocol = new TBinaryProtocol(ttransport);
 * user-generated.thrift.Client client = new user-generated.thrift.Client(tprotocol);
 * client.ping();
 * // execute more works
 * // ...
 * // release
 * ttransport.close();
 * connection.close();
 * transport.shutdownNow();
 * }
 * </pre>
 *
 * @author Bongjae Chang
 */
public class ThriftClientFilter<T extends TServiceClient> extends BaseFilter {

    private static final Logger logger = Grizzly.logger(ThriftClientFilter.class);

    private final Attribute<ObjectPool<SocketAddress, T>> connectionPoolAttribute =
            Grizzly.DEFAULT_ATTRIBUTE_BUILDER.createAttribute(GrizzlyThriftClient.CONNECTION_POOL_ATTRIBUTE_NAME);
    private final Attribute<T> connectionClientAttribute =
            Grizzly.DEFAULT_ATTRIBUTE_BUILDER.createAttribute(GrizzlyThriftClient.CLIENT_ATTRIBUTE_NAME);

    private static final String INPUT_BUFFERS_QUEUE_ATTRIBUTE_NAME = "GrizzlyThriftClient.inputBuffersQueue";
    private final Attribute<BlockingQueue<Buffer>> inputBuffersQueueAttribute =
            Grizzly.DEFAULT_ATTRIBUTE_BUILDER.createAttribute(INPUT_BUFFERS_QUEUE_ATTRIBUTE_NAME,
                    (NullaryFunction<BlockingQueue<Buffer>>) new NullaryFunction<BlockingQueue<Buffer>>() {
                        public BlockingQueue<Buffer> evaluate() {
                            return DataStructures.getLTQInstance();
                        }
                    });

    static final Buffer POISON = Buffers.EMPTY_BUFFER;

    @Override
    public NextAction handleRead(FilterChainContext ctx) throws IOException {
        final Buffer input = ctx.getMessage();
        if (input == null) {
            throw new IOException("input message could not be null");
        }
        if (!input.hasRemaining()) {
            return ctx.getStopAction();
        }
        final Connection connection = ctx.getConnection();
        if (connection == null) {
            throw new IOException("connection must not be null");
        }
        final BlockingQueue<Buffer> inputBuffersQueue = inputBuffersQueueAttribute.get(connection);
        if (inputBuffersQueue == null) {
            throw new IOException("inputBuffersQueue must not be null");
        }
        inputBuffersQueue.offer(input);
        return ctx.getStopAction();
    }

    @SuppressWarnings("unchecked")
    @Override
    public NextAction handleClose(FilterChainContext ctx) throws IOException {
        final Connection<SocketAddress> connection = ctx.getConnection();
        if (connection != null) {
            final ObjectPool<SocketAddress, T> connectionPool = connectionPoolAttribute.remove(connection);
            final T client = connectionClientAttribute.remove(connection);
            if (connectionPool != null && client != null) {
                try {
                    connectionPool.removeObject(connection.getPeerAddress(), client);
                    if (logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, "the connection has been removed in pool. connection={0}", connection);
                    }
                } catch (Exception ignore) {
                }
            }
            final BlockingQueue<Buffer> inputBuffersQueue = inputBuffersQueueAttribute.get(connection);
            if (inputBuffersQueue != null) {
                inputBuffersQueue.clear();
                inputBuffersQueue.offer(POISON);
                inputBuffersQueueAttribute.remove(connection);
            }
        }
        return ctx.getInvokeAction();
    }

    public final BlockingQueue<Buffer> getInputBuffersQueue(final Connection connection) {
        if (connection == null) {
            return null;
        }
        return inputBuffersQueueAttribute.get(connection);
    }
}
