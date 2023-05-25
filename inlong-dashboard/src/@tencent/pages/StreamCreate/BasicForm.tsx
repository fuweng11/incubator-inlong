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

import React, { forwardRef, useImperativeHandle, Ref, useEffect, useState } from 'react';
import {
  Alert,
  Form,
  FormProps,
  Input,
  Segment,
  Switch,
  Select,
  Tooltip,
  Bubble,
} from '@tencent/tea-component';
import { useForm, Controller } from 'react-hook-form';
import { useRequest } from 'ahooks';
import pick from 'lodash/pick';
import { dataLevelMap, DataLevelEnum } from '@/@tencent/enums/stream';
import { useProjectId } from '@/@tencent/components/Use/useProject';
import styles from './index.module.less';

export interface BasicFormRef {
  submit: () => Promise<Record<string, any>>;
  reset: Function;
}

export interface BasicFormProps extends FormProps {
  isUpdate?: boolean;
  savedData?: Record<string, any>;
  onChange?: <T>(values: T) => void;
}

const BasicForm = forwardRef(
  ({ isUpdate, savedData, onChange, ...props }: BasicFormProps, ref: Ref<BasicFormRef>) => {
    const [projectId] = useProjectId();

    const [readyWatch, setReadyWatch] = useState(false);

    const { control, formState, reset, handleSubmit, watch, setValue } = useForm({
      mode: 'onChange',
      defaultValues: isUpdate
        ? undefined
        : {
            name: '',
            dataLevel: DataLevelEnum.L3,
            remark: '',
            sinkInnerHive: false,
            subscribeTHive: {
              inLongNodeName: '',
              dbName: '',
              dbTableName: `table_${new Date().getTime().toString()}`,
            },
          },
    });

    useEffect(() => {
      if (isUpdate && savedData) {
        const values = pick(savedData, [
          'name',
          'dataLevel',
          'remark',
          'sinkInnerHive',
          'subscribeTHive',
        ]);
        reset(values);
      }
    }, [isUpdate, reset, savedData]);

    const { errors } = formState;

    const { data: clusterRes = [] } = useRequest(
      {
        url: '/datasource/search',
        method: 'POST',
        data: {
          projectID: projectId,
          type: 'INNER_THIVE',
          pageSize: 99,
          pageNum: 0,
        },
      },
      {
        onSuccess: result => {
          if (result?.records.length && !isUpdate) {
            setValue('subscribeTHive.inLongNodeName', result?.records[0].name);
          }
        },
      },
    );

    const { data: dbList = [] } = useRequest(
      {
        url: '/project/database/list',
        method: 'POST',
        data: {
          projectID: projectId,
        },
      },
      {
        onSuccess: result => {
          if (result?.length && !isUpdate) {
            setValue('sinkInnerHive', true);
            setValue('subscribeTHive.dbName', result[0].dbName);
          }
          setReadyWatch(true);
        },
      },
    );

    const submit = () => {
      return new Promise((resolve, reject) => {
        handleSubmit(values => {
          resolve({
            ...values,
            autoCreateTable: values.sinkInnerHive,
          });
        }, reject)();
      });
    };

    useImperativeHandle(ref, () => ({
      submit,
      reset,
    }));

    useEffect(() => {
      if (readyWatch) {
        const { unsubscribe } = watch(values => onChange?.(values));
        return () => unsubscribe();
      }
    }, [watch, onChange, readyWatch]);

    return (
      <Form {...props}>
        <Form.Item
          label="数据流名称"
          align="middle"
          required
          status={errors.name?.message ? 'error' : undefined}
          message={errors.name?.message}
        >
          <Controller
            name="name"
            control={control}
            rules={{
              required: '请填写数据流名称',
              pattern: {
                value: /^\w+$/,
                message: '仅支持英文字母、数字、下划线',
              },
            }}
            render={({ field }) => <Input {...field} disabled={isUpdate} />}
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

        <Form.Item
          label="描述"
          align="middle"
          required
          status={errors.remark?.message ? 'error' : undefined}
          message={errors.remark?.message}
        >
          <Controller
            name="remark"
            control={control}
            rules={{ required: '请填写描述' }}
            render={({ field }) => <Input {...field} maxLength={100} />}
          />
        </Form.Item>

        <Form.Item label="默认写入TDW" required>
          <Controller
            name="sinkInnerHive"
            control={control}
            render={({ field }) => (
              <>
                <Tooltip
                  title={
                    dbList?.length
                      ? ''
                      : '暂未申请接入库资源，不支持自动写入TDW，可到项目管理-资源管理中申请'
                  }
                  placement="top"
                >
                  <Switch {...field} disabled={!dbList?.length}>
                    {field.value ? '开启' : '关闭'}
                  </Switch>
                </Tooltip>
                {field.value && (
                  <Alert type="info" hideIcon style={{ marginTop: 10 }}>
                    <ul style={{ listStyle: 'initial' }}>
                      <li>
                        <div style={{ display: 'flex', alignItems: 'center' }}>
                          <span style={{ marginRight: 5 }}>选择集群</span>
                          <Controller
                            name="subscribeTHive.inLongNodeName"
                            control={control}
                            render={({ field }) => (
                              <Select
                                {...field}
                                searchable
                                appearance="default"
                                options={(clusterRes?.records || []).map(item => ({
                                  text: item.displayName,
                                  value: item.name,
                                  tooltip: item.displayName,
                                }))}
                              />
                            )}
                          />
                        </div>
                      </li>
                      <li>
                        开启自动写入TDW，将自动生成表：
                        <Form.Item
                          align="middle"
                          required
                          className={styles.dbTableNameFormItem}
                          status={errors.subscribeTHive?.dbTableName?.message ? 'error' : undefined}
                        >
                          <Controller
                            name="subscribeTHive.dbTableName"
                            control={control}
                            rules={{
                              required: '请填写表名称',
                              pattern: {
                                value: /^\w+$/,
                                message: '仅支持英文字母、数字、下划线',
                              },
                            }}
                            render={({ field }) => (
                              <Bubble error content={errors.subscribeTHive?.dbTableName?.message}>
                                <Input
                                  {...field}
                                  style={{ background: 'transparent', height: 22 }}
                                />
                              </Bubble>
                            )}
                          />
                        </Form.Item>
                        ，表分区默认为“小时”，分区字段：hour，schema与接入源数据字段一致
                      </li>
                      <li>
                        <div style={{ display: 'flex', alignItems: 'center' }}>
                          <span style={{ marginRight: 5 }}>默认选择</span>
                          <Controller
                            name="subscribeTHive.dbName"
                            control={control}
                            render={({ field }) => (
                              <Select
                                {...field}
                                searchable
                                appearance="default"
                                options={dbList.map(item => ({
                                  text: item.dbName,
                                  value: item.dbName,
                                  tooltip: item.dbType,
                                }))}
                              />
                            )}
                          />
                          <span>数据库</span>
                        </div>
                      </li>
                    </ul>
                  </Alert>
                )}
              </>
            )}
          />
        </Form.Item>
      </Form>
    );
  },
);

export default BasicForm;
