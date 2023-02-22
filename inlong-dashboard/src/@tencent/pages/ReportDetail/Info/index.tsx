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

import React, { useState } from 'react';
import { Col, Row, Form, Table, StatusTip } from '@tencent/tea-component';

const Info = () => {
  const fields = [];

  return (
    <Form layout="fixed" style={{ display: 'flex' }} fixedLabelWidth={100}>
      <Row>
        <Col span={24}>
          <Form.Item label="接入方式">
            <Form.Text>SDK</Form.Text>
          </Form.Item>
        </Col>

        <Col span={24}>
          <Form.Title>数据流量</Form.Title>
        </Col>
        <Col span={8}>
          <Form.Item label="单日峰值" tips="tips">
            <Form.Text>Tea</Form.Text>
          </Form.Item>
        </Col>
        <Col span={8}>
          <Form.Item label="单日最大接入量">
            <Form.Text>Tea</Form.Text>
          </Form.Item>
        </Col>
        <Col span={8}>
          <Form.Item label="单条数据最大值">
            <Form.Text>Tea</Form.Text>
          </Form.Item>
        </Col>

        <Col span={24}>
          <Form.Title>数据格式</Form.Title>
        </Col>
        <Col span={8}>
          <Form.Item label="编码类型">
            <Form.Text>Tessssa</Form.Text>
          </Form.Item>
        </Col>
        <Col span={8}>
          <Form.Item label="分隔符">
            <Form.Text>空格</Form.Text>
          </Form.Item>
        </Col>
        <Col span={24}>
          <Form.Item label="数据字段">
            <Table
              bordered
              verticalTop
              records={fields}
              recordKey="instanceId"
              columns={[
                {
                  key: 'fieldName',
                  header: '字段名',
                },
                {
                  key: 'fieldKey',
                  header: '字段类型',
                },
                {
                  key: 'fieldComment',
                  header: '字段描述',
                },
              ]}
              topTip={!fields.length && <StatusTip status="empty" />}
            />
          </Form.Item>
        </Col>
      </Row>
    </Form>
  );
};

export default Info;
