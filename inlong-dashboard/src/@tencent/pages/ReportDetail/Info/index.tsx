/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React from 'react';
import { Col, Row, Form, Table, StatusTip } from '@tencent/tea-component';
import { useRequest } from 'ahooks';
import {
  accessTypeMap,
  encodeTypeMap,
  dataSeparatorMap,
  peakRateMap,
} from '@/@tencent/enums/stream';
import { useProjectId } from '@/@tencent/components/Use/useProject';

const Info = ({ streamId }) => {
  const [projectId] = useProjectId();

  const { data = {} } = useRequest({
    url: '/access/query/info',
    method: 'POST',
    data: {
      projectID: projectId,
      streamID: streamId,
    },
  });

  return (
    <Form layout="fixed" style={{ display: 'flex' }} fixedLabelWidth={100}>
      <Row>
        <Col span={24}>
          <Form.Item label="接入方式">
            <Form.Text>{accessTypeMap.get(data.accessModel) || data.accessModel}</Form.Text>
          </Form.Item>
        </Col>

        <Col span={24}>
          <Form.Title>数据流量</Form.Title>
        </Col>
        <Col span={8}>
          <Form.Item
            label="单日峰值"
            tips="单日峰值（条/秒），请按照数据实际上报量以及后续增长情况，适当上浮20%-50%左右填写，用于容量管理。如果上涨超过容量限制，平台在紧急情况下会抽样或拒绝数据"
          >
            <Form.Text>{peakRateMap.get(data.peakRate) || data.peakRate}</Form.Text>
          </Form.Item>
        </Col>
        <Col span={8}>
          <Form.Item label="单日最大接入量">
            <Form.Text>{data.peakTotalSize} GB</Form.Text>
          </Form.Item>
        </Col>
        <Col span={8}>
          <Form.Item label="单条数据最大值">
            <Form.Text>{data.msgMaxLength} GB</Form.Text>
          </Form.Item>
        </Col>

        <Col span={24}>
          <Form.Title>数据格式</Form.Title>
        </Col>
        <Col span={8}>
          <Form.Item label="编码类型">
            <Form.Text>{encodeTypeMap.get(data.encodeType) || data.encodeType}</Form.Text>
          </Form.Item>
        </Col>
        <Col span={8}>
          <Form.Item label="分隔符">
            <Form.Text>{dataSeparatorMap.get(data.dataSeparator) || data.dataSeparator}</Form.Text>
          </Form.Item>
        </Col>
        <Col span={24}>
          <Form.Item label="数据字段">
            <Table
              bordered
              verticalTop
              records={data.fieldsData || []}
              recordKey="fieldName"
              columns={[
                {
                  key: 'fieldName',
                  header: '字段名',
                },
                {
                  key: 'fieldType',
                  header: '字段类型',
                },
                {
                  key: 'remark',
                  header: '字段描述',
                },
              ]}
              topTip={!data.fieldsData?.length && <StatusTip status="empty" />}
            />
          </Form.Item>
        </Col>
      </Row>
    </Form>
  );
};

export default Info;
