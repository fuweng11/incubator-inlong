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
import React, { useCallback } from 'react';
import { ProTable } from '@tencent/tea-material-pro-table';
import { useState } from 'react';
import AddSubscribeDrawer from './AddSubscribe';
import request from '@/utils/request';
import { useProjectId } from '@/@tencent/components/Use/useProject';
import { dateFormat } from '@/utils';
import { Badge } from '@tencent/tea-component';
import { statusMap } from '@/@tencent/enums/stream';

const SubscribeList = ({ streamId }) => {
  const [projectId] = useProjectId();
  const [records, setRecords] = useState([]);
  const [addDrawerVisible, setAddDrawerVisible] = useState(false);

  const getSubscribeList = useCallback(
    async ({ pageSize, current }) => {
      const data = await request({
        url: '/subscribe/all/list',
        method: 'POST',
        data: {
          projectID: projectId,
          streamID: streamId,
          pageNum: current,
          pageSize,
        },
      });
      return { list: data.records, total: data.total };
    },
    [streamId],
  );

  const onClose = () => {
    setAddDrawerVisible(false);
  };

  return (
    <>
      <ProTable
        recordKey="subscribeID"
        records={records}
        request={async params => {
          const { pageSize, current } = params;
          const { list, total } = await getSubscribeList({ pageSize, current });
          setRecords(list);
          return {
            data: list,
            success: false,
            total: total,
          };
        }}
        operations={[
          {
            type: 'button',
            buttonType: 'primary',
            text: '新增订阅',
            onClick: () => {
              setAddDrawerVisible(true);
            },
          },
        ]}
        columns={[
          { key: 'subscribeID', header: '订阅ID' },
          { key: 'subscribeName', header: '订阅名称' },
          { key: 'subscribeType', header: '写入类型' },
          {
            key: 'status',
            header: '订阅状态',
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
            key: 'action',
            header: '操作',
            render: () => <a>详情</a>,
          },
        ]}
        pageable
      />
      {addDrawerVisible && (
        <AddSubscribeDrawer visible={addDrawerVisible} onClose={onClose} streamId={streamId} />
      )}
    </>
  );
};

export default SubscribeList;
