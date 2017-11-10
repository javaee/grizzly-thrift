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

import org.apache.thrift.transport.TTransportException;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.GrizzlyFuture;
import org.glassfish.grizzly.memory.MemoryManager;
import org.glassfish.grizzly.utils.BufferOutputStream;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.glassfish.grizzly.Processor;
import org.glassfish.grizzly.filterchain.FilterChain;

/**
 * TGrizzlyClientTransport is the client-side TTransport.
 * <p>
 * BlockingQueue which belongs to ThriftClientFilter has input messages when server's response are arrived.
 * Only TTransport#flush() will be called, output messages will be written. Before flush(), output messages will be stored in buffer.
 *
 * @author Bongjae Chang
 */
public class TGrizzlyClientTransport extends AbstractTGrizzlyTransport {

    private static final long DEFAULT_READ_TIMEOUT_MILLIS = -1L; // never timed out
    private static final long DEFAULT_WRITE_TIMEOUT_MILLIS = -1L; // never timed out

    public static TGrizzlyClientTransport create(final Connection connection) {
        return create(connection, DEFAULT_READ_TIMEOUT_MILLIS);
    }

    public static TGrizzlyClientTransport create(final Connection connection, final long readTimeoutMillis) {
        return create(connection, readTimeoutMillis, DEFAULT_WRITE_TIMEOUT_MILLIS);
    }

    public static TGrizzlyClientTransport create(final Connection connection, final long readTimeoutMillis, final long writeTimeoutMillis) {
        if (connection == null) {
            throw new IllegalStateException("connection should not be null.");
        }
        final Processor processor = connection.getProcessor();
        if (!(processor instanceof FilterChain)) {
            throw new IllegalStateException("connection's processor has to be a FilterChain.");
        }
        final FilterChain connectionFilterChain = (FilterChain) connection.getProcessor();
        final int idx = connectionFilterChain.indexOfType(ThriftClientFilter.class);
        if (idx == -1) {
            throw new IllegalStateException("connection has to have ThriftClientFilter in the FilterChain.");
        }
        final ThriftClientFilter thriftClientFilter =
                (ThriftClientFilter) connectionFilterChain.get(idx);
        if (thriftClientFilter == null) {
            throw new IllegalStateException("thriftClientFilter should not be null.");
        }

        @SuppressWarnings("unchecked")
        final BlockingQueue<Buffer> inputBuffersQueue = thriftClientFilter.getInputBuffersQueue(connection);
        if (inputBuffersQueue == null) {
            throw new IllegalStateException("inputBuffersQueue should not be null.");
        }
        return new TGrizzlyClientTransport(connection, inputBuffersQueue, readTimeoutMillis, writeTimeoutMillis);
    }

    private Buffer input = null;
    private final Connection connection;
    private final BlockingQueue<Buffer> inputBuffersQueue;
    private final BufferOutputStream outputStream;
    private final long readTimeoutMillis;
    private final long writeTimeoutMillis;

    private TGrizzlyClientTransport(final Connection connection,
                                    final BlockingQueue<Buffer> inputBuffersQueue,
                                    final long readTimeoutMillis,
                                    final long writeTimeoutMillis) {
        this.connection = connection;
        this.inputBuffersQueue = inputBuffersQueue;
        this.outputStream = new BufferOutputStream(
                connection.getTransport().getMemoryManager()) {

            @Override
            protected Buffer allocateNewBuffer(
                    final MemoryManager memoryManager, final int size) {
                final Buffer b = memoryManager.allocate(size);
                b.allowBufferDispose(true);
                return b;
            }
        };
        this.readTimeoutMillis = readTimeoutMillis;
        this.writeTimeoutMillis = writeTimeoutMillis;
    }

    @Override
    public boolean isOpen() {
        return connection.isOpen();
    }

    @Override
    public void close() {
        final Buffer output = outputStream.getBuffer();
        output.dispose();
        try {
            outputStream.close();
        } catch (IOException ignore) {
        }
        inputBuffersQueue.clear();
        try {
            final GrizzlyFuture closeFuture = connection.close();
            closeFuture.get(3, TimeUnit.SECONDS);
        } catch (Exception ignore) {
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void flush() throws TTransportException {
        checkConnectionOpen();
        final Buffer output = outputStream.getBuffer();
        output.trim();
        outputStream.reset();
        try {
            final GrizzlyFuture future = connection.write(output);
            if (writeTimeoutMillis > 0) {
                future.get(writeTimeoutMillis, TimeUnit.MILLISECONDS);
            } else {
                future.get();
            }
        } catch (TimeoutException te) {
            throw new TTimedoutException(te);
        } catch (ExecutionException ee) {
            throw new TTransportException(ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    public Connection getGrizzlyConnection() {
        return connection;
    }

    @Override
    protected Buffer getInputBuffer() throws TTransportException {
        checkConnectionOpen();
        Buffer localInput = this.input;
        if (localInput == null) {
            localInput = getLocalInput(readTimeoutMillis);
        } else if (localInput.remaining() <= 0) {
            localInput.dispose();
            localInput = getLocalInput(readTimeoutMillis);
        }
        if (localInput == null) {
            throw new TTimedoutException("timed out while reading the input buffer.");
        } else if (localInput == ThriftClientFilter.POISON) {
            throw new TTransportException("client connection was already closed.");
        }
        this.input = localInput;
        return localInput;
    }

    private Buffer getLocalInput(final long readTimeoutMillis) throws TTransportException {
        final Buffer localInput;
        try {
            if (readTimeoutMillis < 0) {
                localInput = inputBuffersQueue.take();
            } else {
                localInput = inputBuffersQueue.poll(readTimeoutMillis, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TTransportException(ie);
        }
        return localInput;
    }

    @Override
    protected BufferOutputStream getOutputStream() {
        return outputStream;
    }

    private void checkConnectionOpen() throws TTransportException {
        if (!isOpen()) {
            throw new TTransportException("client connection is closed.");
        }
    }
}
