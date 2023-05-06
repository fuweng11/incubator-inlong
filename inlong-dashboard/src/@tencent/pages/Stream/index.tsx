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
import request from '@/core/utils/request';
import { dateFormat } from '@/core/utils';
import { PageContainer, Container } from '@/@tencent/components/PageContainer';
import ProCheckbox from '@/@tencent/components/ProCheckbox';
import { statusMap, StatusEnum, AccessTypeEnum } from '@/@tencent/enums/stream';
import { SourceTypeEnum, sourceTypeMap } from '@/@tencent/enums/source';
import { useProjectComputeResources } from '@/@tencent/components/Use/usePlatformAPIs';
import { useProjectId } from '@/@tencent/components/Use/useProject';
import PublishModal from './PublishModal';

const fields: FieldConfig[] = [
  {
    type: 'array',
    component: ProCheckbox,
    name: 'statuses',
    title: '接入状态',
    allOption: true,
    options: Array.from(statusMap).map(([key, ctx]) => ({ name: key, title: ctx.label })),
  },
  {
    type: 'array',
    component: 'selectMultiple',
    name: 'accessModels',
    title: '接入方式',
    appearance: 'button',
    clearable: true,
    groups: AccessTypeEnum,
    options: Array.from(sourceTypeMap).map(([key, ctx]) => ({
      groupKey: key !== SourceTypeEnum.SDK && key !== SourceTypeEnum.FILE ? 'DB' : '',
      value: key,
      text: ctx,
    })),
    allOption: {
      value: 'all',
      text: '全选',
    },
    style: { minWidth: 120 },
  },
  {
    type: 'string',
    name: 'principal',
    title: '负责人',
  },
];

const fieldsDefaultValues = {
  statuses: [],
  accessModels: [],
  principal: '',
};

export default function StreamList() {
  const [projectId] = useProjectId();

  const formRef = useRef<Form>();
  const actionRef = useRef<ActionType>();

  const history = useHistory();

  const [loading, setLoading] = useState<boolean>(false);

  const [options, setOptions] = useState(fieldsDefaultValues);

  const [publishModal, setPublishModal] = useState<{
    visible: boolean;
    id?: number;
    sourceType?: SourceTypeEnum;
  }>({
    visible: false,
  });

  const { data: projectComputeResources } = useProjectComputeResources(projectId);

  const noProjectComputeResources = projectComputeResources
    ? !projectComputeResources.length
    : false;

  const getList = useCallback(
    async options => {
      setLoading(true);
      try {
        const data = await request({
          url: '/access/stream/search',
          method: 'POST',
          data: {
            ...options,
            projectID: projectId,
          },
        });
        return data;
      } finally {
        setLoading(false);
      }
    },
    [projectId],
  );

  return (
    <PageContainer useDefaultContainer={false}>
      {noProjectComputeResources && (
        <Alert type="error">
          项目还未申请计算资源，为保证功能的完整性，请优先申请接入库资源。
          <a
            target="_blank"
            rel="noreferrer"
            href={`/manage/resource/create?ProjectId=${projectId}&from=compute`}
          >
            立即申请
          </a>
        </Alert>
      )}

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
            const data = await getList({ ...options, pageNum: current, pageSize });
            const { records, total } = data;
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
              isLoading: loading,
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
              render: row => sourceTypeMap.get(row.accessModel) || row.accessModel,
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
                      <Badge
                        dot
                        theme={colorTheme === 'error' ? 'danger' : colorTheme}
                        style={{ marginRight: 5 }}
                      />
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
              render: row => row.createTime && dateFormat(new Date(row.createTime)),
            },
            {
              key: 'actions',
              header: '操作',
              render: row => [
                <Button
                  type="link"
                  key="create"
                  onClick={() =>
                    history.push(`/stream/create/${row.streamID}?sourceType=${row.accessModel}`)
                  }
                >
                  编辑
                </Button>,
                <Button
                  type="link"
                  key="detail"
                  onClick={() =>
                    history.push(`/stream/${row.streamID}?sourceType=${row.accessModel}`)
                  }
                >
                  详情
                </Button>,
                row.status === StatusEnum.New && (
                  <Button
                    type="link"
                    key="up"
                    onClick={() =>
                      setPublishModal({
                        visible: true,
                        id: row.streamID,
                        sourceType: row.accessModel,
                      })
                    }
                  >
                    发布上线
                  </Button>
                ),
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
