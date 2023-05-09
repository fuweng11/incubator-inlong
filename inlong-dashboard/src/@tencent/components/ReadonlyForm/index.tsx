/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional defaultValuesrmation
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
import { Col, Row, Form } from '@tencent/tea-component';
import type { FieldItemType } from '@/@tencent/enums/source/common';

export type ReadonlyFormFieldItemType = {
  label: string;
  value: string;
  unit?: string;
  col?: number;
  enumMap?: Map<unknown, unknown>;
  render?: (text: string, row: Record<string, unknown>) => string | React.ReactNode;
};

export interface ReadonlyFormItemType {
  title?: string;
  fields: ReadonlyFormFieldItemType[];
}

export interface ReadonlyFormType {
  conf: ReadonlyFormItemType[];
  defaultValues: Record<string, any>;
  header?: React.ReactNode;
  footer?: React.ReactNode;
}

const ReadonlyForm: React.FC<ReadonlyFormType> = ({ conf, defaultValues, header, footer }) => {
  return (
    <Form layout="fixed" style={{ display: 'flex', marginTop: 10 }} fixedLabelWidth={100}>
      <Row>
        {header && <Col span={24}>{header}</Col>}

        {
          conf.map(item =>
            item.fields?.reduce(
              (acc, cur) =>
                acc.concat(
                  <Col span={cur.col || 8} key={cur.value} style={{ padding: '0 10px' }}>
                    <Form.Item label={cur.label}>
                      <Form.Text>
                        {(() => {
                          if (cur.render) {
                            return cur.render(defaultValues?.[cur.value], cur);
                          }
                          const text = cur.enumMap
                            ? cur.enumMap.get(defaultValues?.[cur.value]) ||
                              defaultValues?.[cur.value]
                            : defaultValues?.[cur.value];
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

        {footer && <Col span={24}>{footer}</Col>}
      </Row>
    </Form>
  );
};

export default ReadonlyForm;
