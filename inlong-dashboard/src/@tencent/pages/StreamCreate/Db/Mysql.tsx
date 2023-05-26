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
import { Form, Input, Radio, Button } from '@tencent/tea-component';
import { Controller } from 'react-hook-form';
import FetchSelect from '@/@tencent/components/FetchSelect';
import request from '@/core/utils/request';
import {
  AllSyncEnum,
  allSyncMap,
  SnapshotModeEnum,
  snapshotModeMap,
} from '@/@tencent/enums/source/mysql';
import { useProjectId } from '@/@tencent/components/Use/useProject';
import { useParams } from 'react-router-dom';

export default function Mysql({ form }) {
  const { control, formState, watch, getValues } = form;
  const { errors } = formState;
  const { id: streamId } = useParams<{ id: string }>();
  const [projectId] = useProjectId();

  const watchAllSync = watch('allSync', AllSyncEnum.YES);

  return (
    <>
      <Form.Item
        label="数据库"
        align="middle"
        required
        suffix={
          <Button
            type="link"
            title="数据源管理"
            onClick={() => window.open(`/manage/data-source?ProjectId=${projectId}`)}
          >
            数据源管理
          </Button>
        }
        status={errors.dataSourceID?.message ? 'error' : undefined}
        message={errors.dataSourceID?.message}
      >
        <Controller
          name="dataSourceID"
          shouldUnregister
          control={control}
          rules={{ required: '请填写数据库' }}
          render={({ field }: any) => {
            if (streamId) {
              const values = getValues();
              field.initOptions = [
                { text: values?.dataBaseName || field.value, value: field.value },
              ];
            }
            return (
              <FetchSelect
                {...field}
                request={async () => {
                  const result = await request({
                    url: '/datasource/search',
                    method: 'POST',
                    data: {
                      projectID: projectId,
                      type: 'MYSQL',
                      pageSize: 99,
                      pageNum: 0,
                    },
                  });
                  return result?.records.map(item => ({
                    text: item.displayName,
                    value: item.name,
                  }));
                }}
              />
            );
          }}
        />
      </Form.Item>

      <Form.Item label="是否整库同步" required>
        <Controller
          name="allSync"
          defaultValue={AllSyncEnum.YES}
          shouldUnregister
          control={control}
          render={({ field }) => (
            <Radio.Group {...field}>
              {Array.from(allSyncMap).map(([key, ctx]) => (
                <Radio name={key as any} key={ctx}>
                  {ctx}
                </Radio>
              ))}
            </Radio.Group>
          )}
        />
      </Form.Item>

      {watchAllSync === AllSyncEnum.NO && (
        <Form.Item
          label="表名白名单"
          tips="白名单应该是一个以逗号分隔的正则表达式列表，与要监控的表的完全限定名称相匹配。表的完全限定名称的格式为 <dbName>.<tableName>，其中 dbName 和 tablename 都可以配置正则表达式。比如：test_db.table*,inlong_db*.user*，表示采集 test_db 库中以 table 开头的所有表 + 以 inlong_db 开头的所有库下的以 user 开头的所有表。"
          align="middle"
          required
          status={errors.tableWhiteList?.message ? 'error' : undefined}
          message={errors.tableWhiteList?.message}
        >
          <Controller
            name="tableWhiteList"
            shouldUnregister
            control={control}
            rules={{ required: '请填写表名白名单' }}
            render={({ field }) => <Input {...field} maxLength={1000} />}
          />
        </Form.Item>
      )}

      <Form.Item
        label="读取方式"
        tips={
          <>
            <div>全量：初始化从日志文件内容第一行开始读取，后续增量读取。</div>
            <div>增量：从日志末尾开始读取最新内容。</div>
          </>
        }
        required
      >
        <Controller
          name="snapshotMode"
          defaultValue={SnapshotModeEnum.FULL}
          shouldUnregister
          control={control}
          render={({ field }) => (
            <Radio.Group {...field}>
              {Array.from(snapshotModeMap).map(([key, ctx]) => (
                <Radio name={key as any} key={ctx}>
                  {ctx}
                </Radio>
              ))}
            </Radio.Group>
          )}
        />
      </Form.Item>
    </>
  );
}
