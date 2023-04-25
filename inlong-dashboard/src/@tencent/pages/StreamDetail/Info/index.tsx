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

import React, { useMemo } from 'react';
import { Col, Row, Form, Table, StatusTip } from '@tencent/tea-component';
import { getFormConf } from './formConf';

const Info = ({ streamId, info }) => {
  const accessModel = info.accessModel;

  const conf = useMemo(() => {
    return getFormConf(accessModel);
  }, [accessModel]);

  return (
    <Form layout="fixed" style={{ display: 'flex', marginTop: 10 }} fixedLabelWidth={100}>
      <Row>
        {
          conf.map(item =>
            item.fields?.reduce(
              (acc, cur) =>
                acc.concat(
                  <Col span={8} key={cur.value} style={{ padding: '0 10px' }}>
                    <Form.Item label={cur.label}>
                      <Form.Text>
                        {(() => {
                          if (cur.render) {
                            return cur.render(info?.[cur.value], cur);
                          }
                          const text = cur.enumMap
                            ? cur.enumMap.get(info?.[cur.value]) || info?.[cur.value]
                            : info?.[cur.value];
                          const unit = cur.unit;
                          return unit ? `${text} ${unit}` : text;
                        })()}
                      </Form.Text>
                    </Form.Item>
                  </Col>,
                ),
              [
                item.title && (
                  <Col span={24} key={item.title} style={{ padding: '0 10px' }}>
                    <Form.Title>{item.title}</Form.Title>
                  </Col>
                ),
              ],
            ),
          ) as any
        }

        <Col span={24}>
          <Form.Item label="数据字段" align="middle">
            <Table
              bordered
              verticalTop
              records={info?.fieldsData || []}
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
              topTip={!info?.fieldsData?.length && <StatusTip status="empty" />}
            />
          </Form.Item>
        </Col>
      </Row>
    </Form>
  );
};

export default Info;
