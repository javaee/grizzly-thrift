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

package org.glassfish.grizzly.thrift.client;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;

/**
 * The interface for managing thrift clients
 *
 * @author Bongjae Chang
 */
public interface ThriftClientManager {
    /**
     * Creates a new {@link ThriftClientBuilder} for the named thrift client to be managed by this thrift client manager.
     * <p>
     * The returned ThriftClientBuilder is associated with this ThriftClientManager.
     * The ThriftClient will be created, added to this ThriftClientManager and started when
     * {@link ThriftClientBuilder#build()} is called.
     *
     * @param thriftClientName    the name of the thrift client to build. A thrift client name must consist of at least one non-whitespace character.
     * @param thriftClientFactory thrift client factory
     * @return the ThriftClientBuilder for the named thrift client
     */
    public <T extends TServiceClient> ThriftClientBuilder createThriftClientBuilder(final String thriftClientName, final TServiceClientFactory<T> thriftClientFactory);

    /**
     * Looks up a named thrift client.
     *
     * @param thriftClientName the name of the thrift client to look for
     * @return the ThriftClient or null if it does exist
     */
    public <T extends TServiceClient> ThriftClient<T> getThriftClient(final String thriftClientName);

    /**
     * Remove a thrift client from the ThriftClientManager. The thrift client will be stopped.
     *
     * @param thriftClientName the thrift client name
     * @return true if the thrift client was removed
     */
    public boolean removeThriftClient(final String thriftClientName);

    /**
     * Shuts down the ThriftClientManager.
     */
    public void shutdown();
}
