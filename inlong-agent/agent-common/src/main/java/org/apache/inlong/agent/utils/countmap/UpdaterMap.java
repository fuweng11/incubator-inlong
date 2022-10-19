/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.agent.utils.countmap;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class UpdaterMap<V1, V2> {

    private final ConcurrentHashMap<String, V2> map = new ConcurrentHashMap();
    private final Updater<V1, V2> p;

    public UpdaterMap(Updater<V1, V2> p) {
        this.p = p;
    }

    public void add(String k, V1 v) {
        synchronized (this.map) {
            if (this.p.inplace() && this.map.containsKey(k)) {
                this.p.update(this.map.get(k), v);
            } else {
                this.map.put(k, this.p.update(this.map.get(k), v));
            }

        }
    }

    public V2 get(String k) {
        synchronized (this.map) {
            return this.map.get(k);
        }
    }

    public V2 getAndSet(String k, V1 v1) {
        synchronized (this.map) {
            V2 v2 = this.map.remove(k);
            this.add(k, v1);
            return v2;
        }
    }

    public V2 remove(String k) {
        synchronized (this.map) {
            return this.map.remove(k);
        }
    }

    public Set<String> keySet() {
        synchronized (this.map) {
            return this.map.keySet();
        }
    }
}
