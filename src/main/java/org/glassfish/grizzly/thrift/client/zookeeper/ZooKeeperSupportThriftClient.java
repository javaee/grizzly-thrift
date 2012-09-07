/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright (c) 2012 Oracle and/or its affiliates. All rights reserved.
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

/**
 * The interface using the ZooKeeper for synchronizing thrift server list
 * <p/>
 * Example of use:
 * {@code
 * final GrizzlyThriftClientManager.Builder managerBuilder = new GrizzlyThriftClientManager.Builder();
 * // setup zookeeper server
 * final ZooKeeperConfig zkConfig = ZooKeeperConfig.create("thrift-client-manager", DEFAULT_ZOOKEEPER_ADDRESS);
 * zkConfig.setRootPath(ROOT);
 * zkConfig.setConnectTimeoutInMillis(3000);
 * zkConfig.setSessionTimeoutInMillis(30000);
 * zkConfig.setCommitDelayTimeInSecs(2);
 * managerBuilder.zooKeeperConfig(zkConfig);
 * // create a thrift client manager
 * final GrizzlyThriftClientManager manager = managerBuilder.build();
 * final GrizzlyThriftClient.Builder<String, String> thriftClientBuilder = manager.createThriftClientBuilder("user");
 * // setup thrift servers
 * final Set<SocketAddress> thriftServers = new HashSet<SocketAddress>();
 * thriftServers.add(THRIFT_SERVER_ADDRESS1);
 * thriftServers.add(THRIFT_SERVER_ADDRESS2);
 * thriftClientBuilder.servers(thriftServers);
 * // create a user thrift
 * final GrizzlyThriftClient<String, String> thriftClient = thriftClientBuilder.build();
 * // ZooKeeperSupportThriftClient's basic operations
 * if (thriftClient.isZooKeeperSupported()) {
 * final String serverListPath = thriftClient.getZooKeeperServerListPath();
 * final String serverList = thriftClient.getCurrentServerListFromZooKeeper();
 * thriftClient.setCurrentServerListOfZooKeeper("localhost:11211,localhost:11212");
 * }
 * // ...
 * // clean
 * manager.removeThriftClient("user");
 * manager.shutdown();
 * }
 *
 * @author Bongjae Chang
 */
public interface ZooKeeperSupportThriftClient {

    /**
     * Check if this thrift client supports the ZooKeeper for synchronizing the thrift server list
     *
     * @return true if this thrift client supports it
     */
    public boolean isZooKeeperSupported();

    /**
     * Return the path of the thrift server list which has been registered in the ZooKeeper server
     *
     * @return the path of the thrift server list in the ZooKeeper server.
     *         "null" means this thrift client doesn't support the ZooKeeper or this thrift client is not started yet
     */
    public String getZooKeeperServerListPath();

    /**
     * Return the current thrift server list string from the ZooKeeper server
     *
     * @return the current server list string
     */
    public String getCurrentServerListFromZooKeeper();

    /**
     * Set the current thrift server list string with the given {@code thriftServerList}
     * <p/>
     * {@code thriftServerList} could be comma separated host:port pairs, each corresponding to a thrift server.
     * e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
     * Be careful that this operation will propagate {@code thriftServerList} to thrift clients which has joinned the same thrift client name(scope)
     * because the thrift server list of ZooKeeper server will be changed.
     *
     * @param thriftServerList the thrift server list string
     * @return true if this thrift server list is set successfully
     */
    public boolean setCurrentServerListOfZooKeeper(final String thriftServerList);

    /**
     * Add the custom {@link BarrierListener}
     *
     * The given {@code listener} will be called after thrift client's default listener will be completed.
     * {@link BarrierListener#onInit} will be called when this thrift client will be registered in the ZooKeeper.
     * {@link BarrierListener#onCommit} will be called when this thrift client's server list will be changed in the ZooKeeper.
     * {@link BarrierListener#onDestroy} will be called when this thrift client will be unregistered in the ZooKeeper.
     *
     * @param listener the custom listener
     */
    public void addZooKeeperListener(final BarrierListener listener);

    /**
     * Remove the custom {@link BarrierListener}
     *
     * The given {@code listener} will be called after thrift client's default listener will be completed.
     * {@link BarrierListener#onInit} will be called when this thrift client will be registered in the ZooKeeper.
     * {@link BarrierListener#onCommit} will be called when this thrift client's server list will be changed in the ZooKeeper.
     * {@link BarrierListener#onDestroy} will be called when this thrift client will be unregistered in the ZooKeeper.
     *
     * @param listener the custom listener which was given by {@link #addZooKeeperListener}
     */
    public void removeZooKeeperListener(final BarrierListener listener);
}
