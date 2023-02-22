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

import React, { useRef, useState } from 'react';
import { Alert, Button, Justify, Status } from '@tencent/tea-component';
import { ProForm, FieldConfig, Form } from '@tencent/tea-material-pro-form';
import { ProTable, ActionType } from '@tencent/tea-material-pro-table';
import { PageContainer, Container } from '@/@tencent/components/PageContainer';

const fields: FieldConfig[] = [
  {
    type: 'array',
    component: 'checkbox',
    name: 'status',
    title: '接入状态',
    options: [
      { name: '', title: '全部' },
      { name: '0', title: '待上线' },
      { name: '1', title: '已上线' },
      { name: '2', title: '异常' },
      { name: '3', title: '下线' },
    ],
  },
  {
    type: 'array',
    component: 'checkbox',
    name: 'type',
    title: '接入方式',
    options: [
      { name: '', title: '全部' },
      { name: 'SDK', title: 'SDK' },
      { name: 'AGENT', title: 'Agent' },
    ],
  },
  {
    type: 'string',
    name: 'owner',
    title: '负责人',
  },
];

const fieldsDefaultValues = {
  status: [''],
  type: [''],
  owner: '',
};

const testData = current =>
  Array.from({ length: 10 }).map((_, index) => ({
    id: (current - 1) * 10 + index,
    name: `Hongkong VPN`,
    status: 'running',
    owner: '香港一区',
    type: 'SDK',
    createTime: '2023-01-01',
  }));

const myQuery = async ({ current }) => {
  const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
  await sleep(1000);
  return { list: testData(current), total: 100 };
};

export default function ProTableExample() {
  const formRef = useRef<Form>();
  const actionRef = useRef<ActionType>();

  const [options, setOptions] = useState(fieldsDefaultValues);

  return (
    <PageContainer useDefaultContainer={false}>
      <Alert type="error">
        项目还未申请计算资源，为保证功能的完整性，请优先申请接入库资源。
        <Button type="link" style={{ textDecoration: 'underline' }}>
          立即申请
        </Button>
      </Alert>

      <Container>
        <div style={{ display: 'flex', marginBottom: -10, background: '#fff' }}>
          <ProForm
            layout="inline"
            fields={fields}
            initialValues={options}
            submitter={false}
            onRef={form => (formRef.current = form)}
            onFormValuesChange={form => setOptions(p => ({ ...p, ...form.values }))}
          />
          <Button style={{ marginRight: 10 }} onClick={() => actionRef.current.reload()}>
            搜索
          </Button>
          <Button onClick={() => formRef.current.reset()}>重置</Button>
        </div>
      </Container>

      <Justify
        left={
          <>
            <Button type="primary">新建接入</Button>
            <Button disabled>接入指引</Button>
          </>
        }
      />
      <div style={{ margin: '0 -20px' }}>
        <ProTable
          actionRef={actionRef}
          request={async params => {
            console.log('request params: ', params, options);
            const { current } = params;
            const { list, total } = await myQuery({ current });
            return {
              data: list,
              success: true,
              total: total,
            };
          }}
          pageable
          addons={[
            {
              type: 'autotip',
              // isLoading: true,
              emptyText: <Status size="s" icon="blank" description="暂无数据" />,
            },
          ]}
          columns={[
            {
              key: 'id',
              header: '数据流 ID',
            },
            {
              key: 'name',
              header: '数据流名称',
            },
            {
              key: 'type',
              header: '接入方式',
            },
            {
              key: 'status',
              header: '接入状态',
              width: 100,
              render: row => {
                if (row.status === 'running') {
                  return <span style={{ color: 'green' }}>运行中</span>;
                }
                if (row.status === 'stopped') {
                  return <span style={{ color: 'red' }}>已关机</span>;
                }
                return row.status;
              },
            },
            {
              key: 'owner',
              header: '负责人',
            },
            {
              key: 'createTime',
              header: '创建时间',
            },
            {
              key: 'actions',
              header: '操作',
              render: row => [
                <Button type="link" key="detail">
                  详情
                </Button>,
                <Button type="link" key="up">
                  发布上线
                </Button>,
              ],
            },
          ]}
        />
      </div>
    </PageContainer>
  );
}
