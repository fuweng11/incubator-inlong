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
import { Form, Input, Radio, Button, TagSelect } from '@tencent/tea-component';
import { Controller } from 'react-hook-form';
import { ReadModeEnum, readModeMap } from '@/@tencent/enums/source/file';
import FetchSelect from '@/@tencent/components/FetchSelect';
import request from '@/core/utils/request';

export default function File({ form }) {
  const { control, formState, watch } = form;
  const { errors } = formState;

  const watchReadMode = watch('readMode', ReadModeEnum.FULL);

  return (
    <>
      <Form.Item
        label="集群名称"
        align="middle"
        required
        suffix={
          <Button type="link" disabled title="暂未支持">
            数据源管理
          </Button>
        }
        status={errors.clusterName?.message ? 'error' : undefined}
        message={errors.clusterName?.message}
      >
        <Controller
          name="clusterName"
          shouldUnregister
          control={control}
          rules={{ required: '请填写集群名称' }}
          render={({ field }) => (
            <FetchSelect
              {...field}
              searchable={false}
              request={async () => {
                const result = await request({
                  url: '/cluster/search',
                  method: 'POST',
                  data: {
                    pageNum: 1,
                    pageSize: 500,
                    type: 'AGENT',
                  },
                });
                return result.records?.map(item => ({
                  text: item.displayName,
                  value: item.name,
                }));
              }}
            />
          )}
        />
      </Form.Item>

      <Form.Item
        label="数据源IP"
        align="middle"
        required
        status={errors.clusterIPs?.message ? 'error' : undefined}
        message={errors.clusterIPs?.message}
      >
        <Controller
          name="clusterIPs"
          shouldUnregister
          control={control}
          rules={{ required: '请填写数据源IP' }}
          render={({ field }) => <TagSelect {...field} tips="" style={{ width: 200 }} />}
        />
      </Form.Item>

      <Form.Item
        label="文件路径"
        align="middle"
        tips="完整路径或通配符：/a/b/*.txt"
        required
        status={errors.filePath?.message ? 'error' : undefined}
        message={errors.filePath?.message}
      >
        <Controller
          name="filePath"
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
        status={errors.readMode?.message ? 'error' : undefined}
        message={errors.readMode?.message}
      >
        <Controller
          name="readMode"
          defaultValue={ReadModeEnum.FULL}
          shouldUnregister
          control={control}
          rules={{ required: '请填写读取方式' }}
          render={({ field }) => (
            <Radio.Group {...field}>
              {Array.from(readModeMap).map(([key, ctx]) => (
                <Radio name={key} key={ctx}>
                  {ctx}
                </Radio>
              ))}
            </Radio.Group>
          )}
        />
      </Form.Item>

      {watchReadMode === ReadModeEnum.DIY && (
        <Form.Item
          label="时间偏移量"
          align="middle"
          tips="从文件的某个时间开始采集，'1m'表示1分钟之后，'-1m'表示1分钟之前，支持m(分钟)，h(小时)，d(天)，空则从当前时间开始采集"
          status={errors.timeOffset?.message ? 'error' : undefined}
          message={errors.timeOffset?.message}
        >
          <Controller
            name="timeOffset"
            shouldUnregister
            control={control}
            render={({ field }) => <Input {...field} />}
          />
        </Form.Item>
      )}
    </>
  );
}
