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
import { AllSyncEnum, SkipDeleteEnum, skipDeleteMap } from '@/@tencent/enums/source/mysql';
import { useProjectId } from '@/@tencent/components/Use/useProject';
import { useParams } from 'react-router-dom';

export default function Mysql({ form }) {
  const { control, formState, watch, getValues } = form;
  const { errors } = formState;
  const { id: streamId } = useParams<{ id: string }>();
  const [projectId] = useProjectId();

  return (
    <>
      <Form.Item
        label="采集器集群"
        align="middle"
        required
        status={errors.inLongClusterName?.message ? 'error' : undefined}
        message={errors.inLongClusterName?.message}
      >
        <Controller
          name="inLongClusterName"
          shouldUnregister
          control={control}
          rules={{ required: '请选择采集器集群' }}
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
        label="数据源名称"
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
          rules={{ required: '请选择数据源' }}
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

      <Form.Item label="表" required>
        <Controller
          name="tableName"
          // defaultValue={AllSyncEnum.YES}
          shouldUnregister
          control={control}
          render={({ field }) => <Input {...field} maxLength={1000} />}
        />
      </Form.Item>

      <Form.Item label="跳过delete事件" required>
        <Controller
          name="skipDelete"
          defaultValue={SkipDeleteEnum.YES}
          shouldUnregister
          control={control}
          render={({ field }) => (
            <Radio.Group {...field}>
              {Array.from(skipDeleteMap).map(([key, ctx]) => (
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
