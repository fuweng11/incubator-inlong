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
import { Form, Input, Radio } from '@tencent/tea-component';
import { Controller } from 'react-hook-form';
import FetchSelect from '@/@tencent/components/FetchSelect';
import request from '@/core/utils/request';
import {
  AllMigrationEnum,
  allMigrationMap,
  ReadTypeEnum,
  readTypeMap,
} from '@/@tencent/enums/source/mysql';

export default function Mysql({ form }) {
  const { control, formState } = form;
  const { errors } = formState;

  return (
    <>
      <Form.Item
        label="数据库"
        align="middle"
        required
        status={errors.dbName?.message ? 'error' : undefined}
        message={errors.dbName?.message}
      >
        <Controller
          name="dbName"
          defaultValue="11"
          shouldUnregister
          control={control}
          rules={{ required: '请填写数据库' }}
          render={({ field }) => (
            <FetchSelect
              {...field}
              request={async () => {
                const result = await request({
                  url: '/test',
                  method: 'POST',
                });
                return result;
              }}
            />
          )}
        />
      </Form.Item>

      <Form.Item label="是否整库迁移" required>
        <Controller
          name="allMigration"
          defaultValue={AllMigrationEnum.YES}
          shouldUnregister
          control={control}
          render={({ field }) => (
            <Radio.Group {...field}>
              {Array.from(allMigrationMap).map(([key, ctx]) => (
                <Radio name={key as any} key={ctx}>
                  {ctx}
                </Radio>
              ))}
            </Radio.Group>
          )}
        />
      </Form.Item>

      <Form.Item
        label="表名白名单"
        tips="白名单应该是一个以逗号分隔的正则表达式列表，与要监控的表的完全限定名称相匹配。表的完全限定名称的格式为 <dbName>.<tableName>，其中 dbName 和 tablename 都可以配置正则表达式。比如：test_db.table*,inlong_db*.user*，表示采集 test_db 库中以 table 开头的所有表 + 以 inlong_db 开头的所有库下的以 user 开头的所有表。"
        align="middle"
        required
        status={errors.tableName?.message ? 'error' : undefined}
        message={errors.tableName?.message}
      >
        <Controller
          name="tableName"
          shouldUnregister
          control={control}
          rules={{ required: '请填写表名白名单' }}
          render={({ field }) => <Input {...field} maxLength={1000} />}
        />
      </Form.Item>

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
          name="readType"
          defaultValue={ReadTypeEnum.FULL}
          shouldUnregister
          control={control}
          render={({ field }) => (
            <Radio.Group {...field}>
              {Array.from(readTypeMap).map(([key, ctx]) => (
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
