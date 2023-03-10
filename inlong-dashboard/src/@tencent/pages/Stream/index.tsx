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

import React, { useRef, useState, useCallback } from 'react';
import { useHistory } from 'react-router-dom';
import { Alert, Button, Justify, Status, Badge } from '@tencent/tea-component';
import { ProForm, FieldConfig, Form } from '@tencent/tea-material-pro-form';
import { ProTable, ActionType } from '@tencent/tea-material-pro-table';
import request from '@/utils/request';
import { PageContainer, Container } from '@/@tencent/components/PageContainer';
import ProCheckbox from '@/@tencent/components/ProCheckbox';
import { statusMap, accessTypeMap } from '@/@tencent/enums/stream';
import PublishModal from './PublishModal';

const fields: FieldConfig[] = [
  {
    type: 'array',
    component: ProCheckbox,
    name: 'status',
    title: '接入状态',
    allOption: true,
    options: Array.from(statusMap).map(([key, ctx]) => ({ name: key, title: ctx.label })),
  },
  {
    type: 'array',
    component: ProCheckbox,
    name: 'accessModel',
    title: '接入方式',
    allOption: true,
    options: Array.from(accessTypeMap).map(([key, ctx]) => ({ name: key, title: ctx })),
  },
  {
    type: 'string',
    name: 'principal',
    title: '负责人',
  },
];

const fieldsDefaultValues = {
  status: [],
  accessModel: [],
  principal: '',
};

const testData = current =>
  Array.from({ length: 10 }).map((_, index) => ({
    streamID: (current - 1) * 10 + index,
    name: `Hongkong VPN`,
    status: index < 3 ? 1 : index < 5 ? 2 : 3,
    principal: '香港一区',
    accessModel: index < 5 ? 1 : 2,
    createTime: '2023-01-01',
  }));

export default function StreamList() {
  const formRef = useRef<Form>();
  const actionRef = useRef<ActionType>();

  const history = useHistory();

  const [options, setOptions] = useState(fieldsDefaultValues);

  const [publishModal, setPublishModal] = useState<{ visible: boolean; id?: number }>({
    visible: false,
  });

  const getList = useCallback(async options => {
    const data = await request({
      url: '/access/stream/search',
      method: 'POST',
      data: {
        ...options,
        projectID: '1608203753111777280',
      },
    });
    return data;
  }, []);

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
            <Button type="primary" onClick={() => history.push('/stream/create')}>
              新建接入
            </Button>
            <Button disabled>接入指引</Button>
          </>
        }
      />
      <div style={{ margin: '0 -20px' }}>
        <ProTable
          actionRef={actionRef}
          request={async params => {
            console.log('request params: ', params, options);
            const { current, pageSize } = params;
            const { records, total } = await getList({ ...options, pageNum: current, pageSize });
            return {
              data: records,
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
              key: 'streamID',
              header: '数据流 ID',
            },
            {
              key: 'name',
              header: '数据流名称',
            },
            {
              key: 'accessModel',
              header: '接入方式',
              render: row => accessTypeMap.get(row.accessModel) || row.accessModel,
            },
            {
              key: 'status',
              header: '接入状态',
              render: row => {
                const ctx = statusMap.get(row.status);
                if (ctx) {
                  const { label, colorTheme } = ctx;
                  return (
                    <span style={{ display: 'flex', alignItems: 'center' }}>
                      <Badge dot theme={colorTheme} style={{ marginRight: 5 }} />
                      <span>{label}</span>
                    </span>
                  );
                }
                return row.status;
              },
            },
            {
              key: 'principal',
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
                <Button
                  type="link"
                  key="detail"
                  onClick={() => history.push(`/stream/${row.streamID}`)}
                >
                  详情
                </Button>,
                <Button
                  type="link"
                  key="up"
                  onClick={() => setPublishModal({ visible: true, id: row.streamID })}
                >
                  发布上线
                </Button>,
              ],
            },
          ]}
        />
      </div>

      <PublishModal
        {...publishModal}
        onOk={data => setPublishModal({ visible: false })}
        onClose={() => setPublishModal({ visible: false })}
      />
    </PageContainer>
  );
}
