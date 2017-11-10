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

package org.glassfish.grizzly.thrift.client.zookeeper;

import java.io.Serializable;

/**
 * The configuration for ZooKeeper client
 * <p>
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
 * // create a user thrift client
 * final GrizzlyThriftClient thriftClient = thriftClientBuilder.build();
 * // ...
 * // clean
 * manager.removeThriftClient("user");
 * manager.shutdown();
 * }
 *
 * @author Bongjae Chang
 */
public class ZooKeeperConfig implements Serializable {

    private static final long serialVersionUID = -3100430916673953287L;

    private static final String DEFAULT_ROOT_PATH = "/";
    private static final long DEFAULT_CONNECT_TIMEOUT_IN_MILLIS = 5000; // 5secs
    private static final long DEFAULT_SESSION_TIMEOUT_IN_MILLIS = 30000; // 30secs
    private static final long DEFAULT_COMMIT_DELAY_TIME_IN_SECS = 60; // 60secs

    private final String name;
    private final String zooKeeperServerList;

    private String rootPath = DEFAULT_ROOT_PATH;
    private long connectTimeoutInMillis = DEFAULT_CONNECT_TIMEOUT_IN_MILLIS;
    private long sessionTimeoutInMillis = DEFAULT_SESSION_TIMEOUT_IN_MILLIS;
    private long commitDelayTimeInSecs = DEFAULT_COMMIT_DELAY_TIME_IN_SECS;

    /**
     * Create ZKClient's configuration with the specific name or Id
     *
     * @param name                name or id
     * @param zooKeeperServerList comma separated host:port pairs, each corresponding to a zookeeper server.
     *                            e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
     * @return ZKClient's configuration
     */
    public static ZooKeeperConfig create(final String name, final String zooKeeperServerList) {
        return new ZooKeeperConfig(name, zooKeeperServerList);
    }


    private ZooKeeperConfig(final String name, final String zooKeeperServerList) {
        this.name = name;
        this.zooKeeperServerList = zooKeeperServerList;
    }

    /**
     * Root path for ZKClient
     *
     * @param rootPath root path of the zookeeper. default is "/".
     */
    public void setRootPath(final String rootPath) {
        this.rootPath = rootPath;
    }

    /**
     * Connect timeout in milli-seconds
     *
     * @param connectTimeoutInMillis connect timeout. negative value means "never timed out". default is 5000(5 secs).
     */
    public void setConnectTimeoutInMillis(final long connectTimeoutInMillis) {
        this.connectTimeoutInMillis = connectTimeoutInMillis;
    }

    /**
     * Session timeout in milli-seconds
     *
     * @param sessionTimeoutInMillis Zookeeper connection's timeout. default is 30000(30 secs).
     */
    public void setSessionTimeoutInMillis(final long sessionTimeoutInMillis) {
        this.sessionTimeoutInMillis = sessionTimeoutInMillis;
    }

    /**
     * Delay time in seconds for committing
     *
     * @param commitDelayTimeInSecs delay time before committing. default is 60(60secs).
     */
    public void setCommitDelayTimeInSecs(final long commitDelayTimeInSecs) {
        this.commitDelayTimeInSecs = commitDelayTimeInSecs;
    }

    public String getName() {
        return name;
    }

    public String getZooKeeperServerList() {
        return zooKeeperServerList;
    }

    public String getRootPath() {
        return rootPath;
    }

    public long getConnectTimeoutInMillis() {
        return connectTimeoutInMillis;
    }

    public long getSessionTimeoutInMillis() {
        return sessionTimeoutInMillis;
    }

    public long getCommitDelayTimeInSecs() {
        return commitDelayTimeInSecs;
    }

    @Override
    public String toString() {
        return "ZooKeeperConfig{" +
                "name='" + name + '\'' +
                ", zooKeeperServerList='" + zooKeeperServerList + '\'' +
                ", rootPath='" + rootPath + '\'' +
                ", connectTimeoutInMillis=" + connectTimeoutInMillis +
                ", sessionTimeoutInMillis=" + sessionTimeoutInMillis +
                ", commitDelayTimeInSecs=" + commitDelayTimeInSecs +
                '}';
    }
}
