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

package org.apache.inlong.manager.pojo.tencent.sc;

import java.util.List;

public class ScPage<T> {

    /**
     * the number of data pieces that meet the query criteria, including unreturned data
     */
    private int totalCount;

    /**
     * the total number of pages of data that meet the conditions, which is used for front-end display paging
     */
    private int totalPages;

    /**
     * the data returned by paging is generally the size of PageSize
     */
    private List<T> data;

    public int getTotalCount() {
        return totalCount;
    }

    public ScPage<T> setTotalCount(int totalCount) {
        this.totalCount = totalCount;
        return this;
    }

    public int getTotalPages() {
        return totalPages;
    }

    public ScPage<T> setTotalPages(int totalPages) {
        this.totalPages = totalPages;
        return this;
    }

    public List<T> getData() {
        return data;
    }

    public ScPage<T> setData(List<T> data) {
        this.data = data;
        return this;
    }
}
