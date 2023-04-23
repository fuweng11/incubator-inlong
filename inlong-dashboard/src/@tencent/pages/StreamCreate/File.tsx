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
import { ReadTypeEnum, readTypeMap } from '@/@tencent/enums/source/file';

export default function File({ form }) {
  const { control, formState } = form;
  const { errors } = formState;

  return (
    <>
      <Form.Item
        label="集群名称"
        align="middle"
        required
        status={errors.cluster?.message ? 'error' : undefined}
        message={errors.cluster?.message}
      >
        <Controller
          name="cluster"
          defaultValue="cluster"
          shouldUnregister
          control={control}
          rules={{ required: '请填写集群名称' }}
          render={({ field }) => <Input {...field} maxLength={100} />}
        />
      </Form.Item>

      <Form.Item
        label="数据源IP"
        align="middle"
        required
        status={errors.ips?.message ? 'error' : undefined}
        message={errors.ips?.message}
      >
        <Controller
          name="ips"
          shouldUnregister
          control={control}
          rules={{ required: '请填写数据源IP' }}
          render={({ field }) => <Input {...field} maxLength={100} />}
        />
      </Form.Item>

      <Form.Item
        label="文件路径"
        align="middle"
        required
        status={errors.path?.message ? 'error' : undefined}
        message={errors.path?.message}
      >
        <Controller
          name="path"
          shouldUnregister
          control={control}
          rules={{ required: '请填写文件路径' }}
          render={({ field }) => <Input {...field} maxLength={100} />}
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
        status={errors.readType?.message ? 'error' : undefined}
        message={errors.readType?.message}
      >
        <Controller
          name="readType"
          defaultValue={ReadTypeEnum.FULL}
          shouldUnregister
          control={control}
          rules={{ required: '请填写读取方式' }}
          render={({ field }) => (
            <Radio.Group {...field}>
              {Array.from(readTypeMap).map(([key, ctx]) => (
                <Radio name={key} key={ctx}>
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