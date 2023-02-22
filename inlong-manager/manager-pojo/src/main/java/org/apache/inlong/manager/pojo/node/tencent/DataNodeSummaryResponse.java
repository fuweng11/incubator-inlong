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

package org.apache.inlong.manager.pojo.node.tencent;


import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Encapsulate information used by the streamSource
 */
@Data
@AllArgsConstructor
@ApiModel("Encapsulate information used by the streamSource")
public class DataNodeSummaryResponse {

    @ApiModelProperty("data node name")
    private String dataNodeName;

    @ApiModelProperty("data node type")
    private String dataNodeType;

    @ApiModelProperty("The business wrapper object used for this node")
    private List<GroupBriefResponse> addGroupBriefResponse;

    public DataNodeSummaryResponse() {
        this.dataNodeName = null;
        this.dataNodeType = null;
        this.addGroupBriefResponse = new ArrayList<>();
    }

    public void addGroupBriefResponse(GroupBriefResponse groupBriefResponse) {
        this.addGroupBriefResponse.add(groupBriefResponse);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GroupBriefResponse {

        @ApiModelProperty("inlong group id")
        private String inlongGroupId;

        @ApiModelProperty("stream info")
        private Set<InlongStreamBriefResponse> inlongStreamBriefResponses;

        @Data
        @NoArgsConstructor
        @AllArgsConstructor
        public static class InlongStreamBriefResponse {

            @ApiModelProperty("inlong group id")
            private String inlongGroupId;

            @ApiModelProperty("inlong stream id")
            private String inlongStreamId;

            @Override
            public boolean equals(Object o) {
                if (this == o) {
                    return true;
                }
                if (!(o instanceof InlongStreamBriefResponse)) {
                    return false;
                }
                InlongStreamBriefResponse that = (InlongStreamBriefResponse) o;
                return Objects.equals(inlongGroupId, that.inlongGroupId) && Objects.equals(
                        inlongStreamId, that.inlongStreamId);
            }

            @Override
            public int hashCode() {
                return Objects.hash(inlongGroupId, inlongStreamId);
            }
        }

    }

}
