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
import { ProTable } from '@tencent/tea-material-pro-table';
import { useState } from 'react';
import AddSubscribeDrawer from './AddSubscribe';

const defaultRecords = [
  {
    id: 1,
    name: 'test',
    writeType: 'Hive',
    type: 'running',
    owner: 'jinghao',
    createTime: '2022-01-01 12:00:00',
  },
];

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const myQuery = async ({ pageSize, current }) => {
  await sleep(1000);
  return { list: defaultRecords, total: 1 };
};

const SubscribeList = () => {
  const [records, setRecords] = useState([]);
  const [addDrawerVisible, setAddDrawerVisible] = useState(false);

  const onClose = () => {
    setAddDrawerVisible(false);
  };

  return (
    <>
      <ProTable
        records={records}
        request={async params => {
          console.log('request params: ', params);
          const { pageSize, current } = params;
          const { list, total } = await myQuery({ pageSize, current });
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
          { key: 'id', header: '订阅ID' },
          { key: 'name', header: '订阅名称' },
          { key: 'writeType', header: '写入类型' },
          {
            key: 'type',
            header: '订阅状态',
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
            key: 'action',
            header: '操作',
            render: () => <a>详情</a>,
          },
        ]}
        pageable
      />
      {addDrawerVisible && <AddSubscribeDrawer visible={addDrawerVisible} onClose={onClose} />}
    </>
  );
};

export default SubscribeList;
