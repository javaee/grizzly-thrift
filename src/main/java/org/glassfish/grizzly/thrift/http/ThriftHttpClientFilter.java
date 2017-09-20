/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2014-2017 Oracle and/or its affiliates. All rights reserved.
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

package org.glassfish.grizzly.thrift.http;

import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.http.HttpBaseFilter;
import org.glassfish.grizzly.http.HttpContent;
import org.glassfish.grizzly.http.HttpRequestPacket;
import org.glassfish.grizzly.http.Method;
import org.glassfish.grizzly.http.Protocol;
import org.glassfish.grizzly.http.util.Header;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * ThriftHttpClientFilter is a client-side filter for Thrift RPC processors over HTTP.
 * <p>
 * Usages:
 * <pre>
 * {@code
 * final FilterChainBuilder clientFilterChainBuilder = FilterChainBuilder.stateless();
 * clientFilterChainBuilder.add(new TransportFilter()).add(new HttpClientFilter()).add(new ThriftHttpClientFilter("/yourUriPath")).add(new ThriftClientFilter());
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
public class ThriftHttpClientFilter extends HttpBaseFilter {

    private static final String THRIFT_HTTP_CONTENT_TYPE = "application/x-thrift";

    private final String uriPath;

    public ThriftHttpClientFilter(final String uriPath) {
        this.uriPath = uriPath;
    }

    @Override
    public NextAction handleRead(FilterChainContext ctx) throws IOException {
        final HttpContent httpContent = ctx.getMessage();
        if (httpContent == null) {
            throw new IOException("httpContent should not be null");
        }
        final Buffer responseBodyBuffer = httpContent.getContent();
        ctx.setMessage(responseBodyBuffer);
        return ctx.getInvokeAction();
    }

    @Override
    public NextAction handleWrite(FilterChainContext ctx) throws IOException {
        final Buffer requestBodyBuffer = ctx.getMessage();
        if (requestBodyBuffer == null) {
            throw new IOException("request body's buffer should not be null");
        }

        final HttpRequestPacket.Builder builder = HttpRequestPacket.builder();
        builder.method(Method.POST);
        builder.protocol(Protocol.HTTP_1_1);
        builder.uri(uriPath);
        final InetSocketAddress peerAddress = (InetSocketAddress) ctx.getConnection().getPeerAddress();
        final String httpHost = peerAddress.getHostName() + ':' + peerAddress.getPort();
        builder.host(httpHost);
        final long contentLength = requestBodyBuffer.remaining();
        if (contentLength >= 0) {
            builder.contentLength(contentLength);
        } else {
            builder.chunked(true);
        }
        builder.contentType(THRIFT_HTTP_CONTENT_TYPE);
        builder.header(Header.Connection, "keep-alive");
        builder.header(Header.Accept, "*/*");
        builder.header(Header.UserAgent, "grizzly-thrift");
        final HttpRequestPacket requestPacket = builder.build();

        final HttpContent content = requestPacket.httpContentBuilder().content(requestBodyBuffer).build();
        content.setLast(true);
        ctx.setMessage(content);
        return ctx.getInvokeAction();
    }
}
