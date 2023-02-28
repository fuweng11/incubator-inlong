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

import React, { useEffect } from 'react';
import {
  Modal,
  ModalProps,
  Button,
  Col,
  Row,
  Form,
  Table,
  StatusTip,
  Input,
} from '@tencent/tea-component';
import { useForm, Controller } from 'react-hook-form';

interface FieldsParseProps extends ModalProps {
  id?: number | string;
  onOk: (values: Record<string, unknown>) => void;
}

const conf = [
  {
    title: '基本信息',
    fields: [
      { label: '数据流中文名', value: 'cnName' },
      { label: '数据流英文名', value: 'name' },
      { label: '数据流ID', value: 'id' },
      { label: '创建人', value: 'creator' },
      { label: '创建时间', value: 'createTime' },
      { label: '数据分级', value: 'level' },
      { label: '描述', value: 'desc' },
    ],
  },
  {
    title: '接入信息',
    fields: [{ label: '接入方式', value: 'type' }],
  },
  {
    title: '数据流量',
    fields: [
      { label: '单日峰值', value: 'a' },
      { label: '单日最大接入量', value: 'b' },
      { label: '单条数据最大值', value: 'c' },
    ],
  },
  {
    title: '数据格式',
    fields: [
      { label: '编码类型', value: 'a' },
      { label: '分隔符', value: 'b' },
    ],
  },
];

const FieldsParse: React.FC<FieldsParseProps> = ({ id, visible, onOk, onClose, ...rest }) => {
  const { control, formState, reset, handleSubmit } = useForm({
    mode: 'onChange',
    defaultValues: {
      remark: '',
    },
  });

  const { errors } = formState;

  const fields = [];

  const handleOk = handleSubmit(values => {
    onOk(values);
  });

  useEffect(() => {
    if (visible) {
      console.log('open');
    } else {
      reset();
    }
  }, [visible, reset]);

  return (
    <Modal size="xl" caption="发布上线审批" visible={visible} onClose={onClose} {...rest}>
      <Modal.Body>
        <Form layout="fixed" style={{ display: 'flex' }} fixedLabelWidth={100}>
          <Row>
            {
              conf.map(item =>
                item.fields.reduce(
                  (acc, cur) =>
                    acc.concat(
                      <Col span={8} key={cur.value} style={{ padding: '0 10px' }}>
                        <Form.Item label={cur.label}>
                          <Form.Text>{cur.value}</Form.Text>
                        </Form.Item>
                      </Col>,
                    ),
                  [
                    <Col span={24} key={item.title} style={{ padding: '0 10px' }}>
                      <Form.Title>{item.title}</Form.Title>
                    </Col>,
                  ],
                ),
              ) as any
            }

            <Col span={24}>
              <Form.Item label="数据字段" align="middle">
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

              <Form.Item
                label="申请原因"
                align="middle"
                required
                status={errors.remark?.message ? 'error' : undefined}
                message={errors.remark?.message}
              >
                <Controller
                  name="remark"
                  control={control}
                  rules={{ validate: value => (value ? undefined : '请填写申请原因') }}
                  render={({ field }) => (
                    <Input.TextArea
                      {...field}
                      style={{ width: '100%' }}
                      placeholder="请输入申请原因，不超过100个字"
                      maxLength={100}
                    />
                  )}
                />
              </Form.Item>
            </Col>
          </Row>
        </Form>
      </Modal.Body>

      <Modal.Footer>
        <Button type="primary" onClick={handleOk}>
          确定
        </Button>
        <Button type="weak" onClick={onClose}>
          取消
        </Button>
      </Modal.Footer>
    </Modal>
  );
};

export default FieldsParse;
