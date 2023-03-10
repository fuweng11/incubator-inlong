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

import React, { forwardRef, useImperativeHandle, Ref, useEffect } from 'react';
import { Alert, Form, FormProps, Input, Segment, Switch, Select } from '@tencent/tea-component';
import { useForm, Controller } from 'react-hook-form';
import { useRequest } from 'ahooks';
import { dataLevelMap, DataLevelEnum } from '@/@tencent/enums/stream';

export interface BasicFormRef {
  submit: () => Promise<Record<string, any>>;
  reset: Function;
}

export interface BasicFormProps extends FormProps {
  onChange?: <T>(values: T) => void;
}

const BasicForm = forwardRef(({ onChange, ...props }: BasicFormProps, ref: Ref<BasicFormRef>) => {
  const { control, formState, reset, handleSubmit, watch } = useForm({
    mode: 'onChange',
    defaultValues: {
      name: '',
      dataLevel: DataLevelEnum.L3,
      remark: '',
      autoCreateTable: true,
      dbName: '',
    },
  });

  const { errors } = formState;

  const { data: dbList = [] } = useRequest({
    url: '/access/querydblist',
    method: 'POST',
  });

  const submit = () => {
    return new Promise((resolve, reject) => {
      handleSubmit(values => {
        resolve({
          ...values,
          dbTableName: values.dbName,
        });
      }, reject)();
    });
  };

  useImperativeHandle(ref, () => ({
    submit,
    reset,
  }));

  useEffect(() => {
    const { unsubscribe } = watch(values => onChange?.(values));
    return () => unsubscribe();
  }, [watch, onChange]);

  return (
    <Form {...props}>
      <Form.Item
        label="数据流英文名"
        align="middle"
        required
        status={errors.name?.message ? 'error' : undefined}
        message={errors.name?.message}
      >
        <Controller
          name="name"
          control={control}
          rules={{
            required: '请填写数据流英文名',
            pattern: {
              value: /^[a-zA-z_]+$/,
              message: '仅支持英文字母、下划线',
            },
          }}
          render={({ field }) => <Input {...field} />}
        />
      </Form.Item>

      <Form.Item
        label="数据分级"
        align="middle"
        tips="一级最高，五级最低，级别越高成本越高。"
        required
        status={errors.dataLevel?.message ? 'error' : undefined}
        message={errors.dataLevel?.message}
      >
        <Controller
          name="dataLevel"
          control={control}
          rules={{ required: '请填写数据分级' }}
          render={({ field }) => (
            <Segment
              {...(field as any)}
              options={Array.from(dataLevelMap).map(([key, ctx]) => ({
                value: key,
                text: key === DataLevelEnum.L3 ? `${ctx}（推荐）` : ctx,
              }))}
            />
          )}
        />
      </Form.Item>

      <Form.Item label="描述" align="middle">
        <Controller name="remark" control={control} render={({ field }) => <Input {...field} />} />
      </Form.Item>

      <Form.Item label="默认写入TDW" required>
        <Controller
          name="autoCreateTable"
          control={control}
          render={({ field }) => <Switch {...field} />}
        />
        <Alert type="info" hideIcon style={{ marginTop: 10 }}>
          <ul style={{ listStyle: 'initial' }}>
            <li>
              开启自动写入TDW,将自动生成一张和日志英文名同名的Hive表，表分区默认为“小时”，分区字段：hour，schema与接入源数据字段一致
            </li>
            <li>
              <div style={{ display: 'flex', alignItems: 'center' }}>
                <span style={{ marginRight: 5 }}>默认选择</span>
                <Controller
                  name="dbName"
                  control={control}
                  render={({ field }) => (
                    <Select
                      {...field}
                      appearance="default"
                      options={dbList.map(item => ({
                        text: item,
                        value: item,
                      }))}
                    />
                  )}
                />
                <span>数据库</span>
              </div>
            </li>
          </ul>
        </Alert>
      </Form.Item>
    </Form>
  );
});

export default BasicForm;
