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
import InfoCard from '@/@tencent/components/InfoCard';
import { Drawer, Button, Icon, Row, Col, Form } from '@tencent/tea-component';
import { ProForm, ProFormProps } from '@tencent/tea-material-pro-form';
import React, { useRef } from 'react';

export interface AddSubscribeDrawerProps {
  visible: boolean;
  onClose: () => void;
}

const formFields: ProFormProps['fields'] = [
  {
    name: 'writeType',
    type: 'string',
    title: '写入类型',
    required: true,
    component: 'select',
    appearance: 'button',
    style: { width: '80%' },
    defaultValue: 'HIVE',
    disabled: true,
    options: [
      {
        value: 'HIVE',
        text: 'HIVE',
      },
    ],
  },
  {
    name: 'sperate',
    type: 'string',
    title: '分区间隔',
    required: true,
    component: 'segment',
    defaultValue: 'hour',
    options: [
      {
        value: 'hour',
        text: '小时',
      },
      {
        value: 'day',
        text: '天',
      },
    ],
    suffix: <span>分区字段：</span>,
  },
  {
    name: 'database',
    type: 'string',
    title: '数据库名称',
    required: true,
    component: 'select',
    appearance: 'button',
    style: { width: '80%' },
    defaultValue: 'HIVE',
    options: [
      {
        value: 'HIVE',
        text: 'HIVE',
      },
    ],
    suffix: (
      <a href="/" target="_blank">
        申请资源
      </a>
    ),
  },
  {
    name: 'tableType',
    type: 'string',
    title: '选择表',
    required: true,
    component: 'segment',
    defaultValue: 'auto',
    options: [
      {
        value: 'auto',
        text: '自动建表',
      },
      {
        value: 'select',
        text: '选择已有表',
      },
    ],
  },
  {
    name: 'table',
    title: '表名称',
    type: 'string',
    required: true,
    component: 'input',
    style: { width: '80%' },
    defaultValue: 'test_table_name',
    options: [
      {
        value: 'test_table_name',
        text: 'test_table_name',
      },
    ],
    visible: values => values.tableType === 'auto',
  },
  {
    name: 'tableName',
    title: '表名称',
    type: 'string',
    required: true,
    component: 'select',
    appearance: 'button',
    style: { width: '80%' },
    defaultValue: 'test_table_name',
    options: [
      {
        value: 'test_table_name',
        text: 'test_table_name',
      },
    ],
    visible: values => values.tableType === 'select',
  },
];

const AddSubscribeDrawer = ({ visible, onClose }: AddSubscribeDrawerProps) => {
  const formRef = useRef<any>();
  const handleOk = () => {
    formRef.current.submit(values => {
      console.log(values);
    });
  };
  return (
    <Drawer
      style={{ width: '100%', zIndex: 1000 }}
      size="l"
      placement="right"
      disableAnimation={false}
      outerClickClosable={true}
      showMask={true}
      visible={visible}
      title="新增订阅"
      footer={
        <div style={{ float: 'right' }}>
          <Button type="primary" onClick={handleOk}>
            新增
          </Button>
          <Button style={{ marginLeft: '10px' }} type="weak" onClick={onClose}>
            取消
          </Button>
        </div>
      }
      onClose={onClose}
    >
      <Row style={{ padding: '0 8%' }}>
        <Col span={24}>
          <p style={{ fontSize: '16px', fontWeight: 'bold' }}>配置数据源信息</p>
        </Col>
        <Col span={11}>
          <InfoCard title="数据来源" icon={<Icon type="table"></Icon>} style={{ height: '300px' }}>
            <Form>
              <Form.Item label="接入方式">
                <Form.Text>SDK</Form.Text>
              </Form.Item>
              <Form.Item label="单日峰值" tips={'tips文案 todo'}>
                <Form.Text>Tea</Form.Text>
              </Form.Item>
              <Form.Item label="单日最大存储量">
                <Form.Text>1000G</Form.Text>
              </Form.Item>
              <Form.Item label="平均每条数据大小">
                <Form.Text>1GB</Form.Text>
              </Form.Item>
              <Form.Item label="采集类型">
                <Form.Text>UTF-8</Form.Text>
              </Form.Item>
              <Form.Item label="分隔符">
                <Form.Text>分号</Form.Text>
              </Form.Item>
            </Form>
          </InfoCard>
        </Col>
        <Col span={2}></Col>
        <Col span={11}>
          <InfoCard title="写入信息" icon={<Icon type="table"></Icon>} style={{ height: '300px' }}>
            <ProForm
              fields={formFields}
              submitter={false}
              onRef={form => (formRef.current = form)}
            />
          </InfoCard>
        </Col>
        <Col span={24}>
          <p style={{ fontSize: '16px', fontWeight: 'bold' }}>字段映射</p>
          <p>选择需要订阅的来源字段，并对齐写入字段，注意：请确定字段为一一映射关系。</p>
        </Col>
      </Row>
    </Drawer>
  );
};

export default AddSubscribeDrawer;
