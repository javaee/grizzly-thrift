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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Returns the stored value by round-robin
 * <p>
 * All operations can be called dynamically and are thread-safe.
 *
 * @author Bongjae Chang
 */
public class RoundRobinStore<T> {

    private final List<T> valueList = new ArrayList<T>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Lock rLock = lock.readLock();
    private final Lock wLock = lock.writeLock();
    private int counter = 0;

    public RoundRobinStore() {
    }

    public RoundRobinStore(final Set<T> initSet, final boolean shuffle) {
        configure(initSet, shuffle);
    }

    public void configure(final Set<T> initSet, final boolean shuffle) {
        if (initSet != null && !initSet.isEmpty()) {
            wLock.lock();
            try {
                valueList.addAll(initSet);
                if (shuffle) {
                    Collections.shuffle(valueList);
                }
            } finally {
                wLock.unlock();
            }
        }
    }

    public void shuffle() {
        wLock.lock();
        try {
            if (!valueList.isEmpty()) {
                Collections.shuffle(valueList);
            }
        } finally {
            wLock.unlock();
        }
    }

    public void add(final T value) {
        wLock.lock();
        try {
            valueList.add(value);
        } finally {
            wLock.unlock();
        }
    }

    public void remove(final T value) {
        wLock.lock();
        try {
            valueList.remove(value);
        } finally {
            wLock.unlock();
        }
    }

    public T get() {
        rLock.lock();
        try {
            final int valueSize = valueList.size();
            if (valueSize <= 0) {
                return null;
            }
            final int index = (counter++ & 0x7fffffff) % valueSize;
            return valueList.get(index);
        } finally {
            rLock.unlock();
        }
    }

    public boolean hasValue(final T value) {
        if (value == null) {
            return false;
        }
        rLock.lock();
        try {
            return valueList.contains(value);
        } finally {
            rLock.unlock();
        }
    }

    public boolean hasOnly(final T value) {
        if (value == null) {
            return false;
        }
        rLock.lock();
        try {
            return valueList.size() == 1 && valueList.contains(value);
        } finally {
            rLock.unlock();
        }
    }

    public int size() {
        rLock.lock();
        try {
            return valueList.size();
        } finally {
            rLock.unlock();
        }

    }

    public void clear() {
        wLock.lock();
        try {
            valueList.clear();
        } finally {
            wLock.unlock();
        }
    }
}
