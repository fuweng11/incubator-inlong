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

package org.apache.inlong.manager.dao.entity;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * Stream sink entity, including sink type, sink name, etc.
 */
@Data
public class StreamSinkEntity implements Serializable {

    private static final long serialVersionUID = 1L;
    private Integer id;
    private String inlongGroupId;
    private String inlongStreamId;
    private String sinkType;
    private String sinkName;
    private String description;
    private String inlongClusterName;
    private String dataNodeName;
    private String sortTaskName;
    private String sortConsumerGroup;
    private Integer enableCreateResource;

    private String operateLog;
    private Integer status;
    private Integer previousStatus;
    private Integer isDeleted;
    private String creator;
    private String modifier;
    private Date createTime;
    private Date modifyTime;

    // Another fields saved as JSON string in extParams
    private String extParams;

    private Integer version;

}