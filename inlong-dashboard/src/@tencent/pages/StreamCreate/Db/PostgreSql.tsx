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
import { ReadTypeEnum, readTypeMap } from '@/@tencent/enums/source/postgreSql';
import { useProjectId } from '@/@tencent/components/Use/useProject';

export default function PostgreSql({ form }) {
  const { control, formState } = form;
  const { errors } = formState;

  const [projectId] = useProjectId();

  return (
    <>
      <Form.Item
        label="数据库"
        align="middle"
        required
        suffix={
          <Button type="link" disabled title="暂未支持">
            数据源管理
          </Button>
        }
        status={errors.dataBaseName?.message ? 'error' : undefined}
        message={errors.dataBaseName?.message}
      >
        <Controller
          name="dataBaseName"
          shouldUnregister
          control={control}
          rules={{ required: '请填写数据库' }}
          render={({ field }) => (
            <FetchSelect
              {...field}
              request={async () => {
                const result = await request({
                  url: '/project/database/list',
                  method: 'POST',
                  data: {
                    projectID: projectId,
                  },
                });
                return result?.map(item => ({
                  text: item.dbName,
                  value: item.dbName,
                }));
              }}
            />
          )}
        />
      </Form.Item>

      <Form.Item
        label="模式"
        align="middle"
        required
        status={errors.schema?.message ? 'error' : undefined}
        message={errors.schema?.message}
      >
        <Controller
          name="schema"
          shouldUnregister
          control={control}
          rules={{ required: '请填写模式' }}
          render={({ field }) => <Input {...field} maxLength={1000} />}
        />
      </Form.Item>

      <Form.Item
        label="表"
        align="middle"
        required
        status={errors.tableName?.message ? 'error' : undefined}
        message={errors.tableName?.message}
      >
        <Controller
          name="tableName"
          shouldUnregister
          control={control}
          rules={{ required: '请填写表' }}
          render={({ field }) => (
            <Input {...field} />
            // <FetchSelect
            //   {...field}
            //   request={async () => {
            //     const result = await request({
            //       url: '/test',
            //       method: 'POST',
            //     });
            //     return result;
            //   }}
            // />
          )}
        />
      </Form.Item>

      <Form.Item
        label="主键"
        align="middle"
        required
        status={errors.primaryKey?.message ? 'error' : undefined}
        message={errors.primaryKey?.message}
      >
        <Controller
          name="primaryKey"
          shouldUnregister
          control={control}
          rules={{ required: '请填写主键' }}
          render={({ field }) => <Input {...field} maxLength={100} />}
        />
      </Form.Item>

      <Form.Item label="读取方式" required>
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
