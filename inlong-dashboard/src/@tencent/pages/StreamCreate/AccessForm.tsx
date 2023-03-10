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

import React, { useState, forwardRef, useImperativeHandle, Ref, useRef, useEffect } from 'react';
import {
  Form,
  FormProps,
  InputNumber,
  Segment,
  Select,
  Input,
  Button,
  Icon,
} from '@tencent/tea-component';
import { useForm, Controller } from 'react-hook-form';
import {
  accessTypeMap,
  AccessTypeEnum,
  dayMaxMap,
  DayMaxEnum,
  dataEncodingMap,
  DataEncodingEnum,
  dataSeparatorMap,
  DataSeparatorEnum,
} from '@/@tencent/enums/stream';
import FieldsParse from '@/@tencent/components/FieldsParse';
import EditableTable, { addons, EditableTableRef } from '@/@tencent/components/EditableTable';

export interface AccessFormRef {
  submit: () => Promise<Record<string, any>>;
  reset: Function;
}

export interface AccessFormProps extends FormProps {
  onChange?: <T>(values: T) => void;
}

const AccessForm = forwardRef(
  ({ onChange, ...props }: AccessFormProps, ref: Ref<AccessFormRef>) => {
    const editableTable = useRef<EditableTableRef>();

    const { control, formState, reset, handleSubmit, watch } = useForm({
      mode: 'onChange',
      defaultValues: {
        accessType: AccessTypeEnum.SDK,
        dayMax: DayMaxEnum.L2,
        n1: 20,
        n2: 20,
        dataEncoding: DataEncodingEnum.UTF8,
        dataSeparator: DataSeparatorEnum.Comma,
      },
    });

    const { errors } = formState;

    const [fieldsParse, setFieldsParse] = useState<{ visible: boolean }>({
      visible: false,
    });

    const submit = async () => {
      const [v1, v2] = await Promise.all([
        new Promise<Record<string, any>>((resolve, reject) => {
          handleSubmit(values => {
            resolve(values);
          }, reject)();
        }),
        editableTable.current.submit(),
      ]);
      const values = { ...v1, fields: v2 };
      return values;
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
          label="接入方式"
          align="middle"
          required
          status={errors.accessType?.message ? 'error' : undefined}
          message={errors.accessType?.message}
        >
          <Controller
            name="accessType"
            control={control}
            rules={{ validate: value => (value ? undefined : '请填写接入方式') }}
            render={({ field }) => (
              <Segment
                {...field}
                options={Array.from(accessTypeMap).map(([key, ctx]) => ({ value: key, text: ctx }))}
              />
            )}
          />
        </Form.Item>

        <Form.Title>数据流量</Form.Title>

        <Form.Item
          label="单日峰值"
          align="middle"
          tips="单日峰值（条/秒），请按照数据实际上报量以及后续增长情况，适当上浮20%-50%左右填写， 用于容量管理。如果上涨超过容量限制，平台在紧急情况下会抽样或拒绝数据"
          required
          status={errors.dayMax?.message ? 'error' : undefined}
          message={errors.dayMax?.message}
        >
          <Controller
            name="dayMax"
            control={control}
            rules={{ validate: value => (value ? undefined : '请填写单日峰值') }}
            render={({ field }) => (
              <Select
                {...field}
                appearance="button"
                options={Array.from(dayMaxMap).map(([key, ctx]) => ({
                  value: key,
                  text: ctx,
                }))}
              />
            )}
          />
        </Form.Item>

        <Form.Item
          label="单日最大接入量"
          align="middle"
          suffix="GB"
          required
          status={errors.n1?.message ? 'error' : undefined}
          message={errors.n1?.message}
        >
          <Controller
            name="n1"
            control={control}
            rules={{ validate: value => (value ? undefined : '请填写单日最大接入量') }}
            render={({ field }) => <InputNumber {...field} />}
          />
        </Form.Item>

        <Form.Item
          label="单条数据最大值"
          align="middle"
          suffix="GB"
          required
          status={errors.n2?.message ? 'error' : undefined}
          message={errors.n2?.message}
        >
          <Controller
            name="n2"
            control={control}
            rules={{ validate: value => (value ? undefined : '请填写单条数据最大值') }}
            render={({ field }) => <InputNumber {...field} />}
          />
        </Form.Item>

        <Form.Title>数据格式</Form.Title>

        <Form.Item
          label="编码类型"
          align="middle"
          required
          status={errors.dataEncoding?.message ? 'error' : undefined}
          message={errors.dataEncoding?.message}
        >
          <Controller
            name="dataEncoding"
            control={control}
            rules={{ validate: value => (value ? undefined : '请填写编码类型') }}
            render={({ field }) => (
              <Segment
                {...field}
                options={Array.from(dataEncodingMap).map(([key, ctx]) => ({
                  value: key,
                  text: ctx,
                }))}
              />
            )}
          />
        </Form.Item>

        <Form.Item
          label="分隔符"
          align="middle"
          required
          status={errors.dataSeparator?.message ? 'error' : undefined}
          message={errors.dataSeparator?.message}
        >
          <Controller
            name="dataSeparator"
            control={control}
            rules={{ validate: value => (value ? undefined : '请填写分隔符') }}
            render={({ field }) => (
              <Segment
                {...field}
                options={Array.from(dataSeparatorMap).map(([key, ctx]) => ({
                  value: key,
                  text: ctx,
                }))}
              />
            )}
          />
        </Form.Item>

        <Form.Item label="数据字段" required>
          <Button
            type="link"
            style={{ display: 'flex', alignItems: 'center' }}
            onClick={() => setFieldsParse({ visible: true })}
          >
            <Icon type="plus" />
            批量解析字段
          </Button>
          <EditableTable
            ref={editableTable}
            showRowIndex
            addons={[
              addons.scrollable({
                maxHeight: 310,
              }),
            ]}
            defaultValues={[
              {
                fieldName: '',
                fieldType: '',
                fieldComment: '',
              },
            ]}
            columns={[
              {
                key: 'fieldName',
                header: '字段名',
                rules: { validate: value => (value ? undefined : '请填写字段名') },
                render: ({ field }) => <Input {...field} placeholder="请输入字段名" />,
              },
              {
                key: 'fieldType',
                header: '类型',
                rules: { validate: value => (value ? undefined : '请填写类型') },
                render: ({ field }) => (
                  <Select
                    {...field}
                    appearance="button"
                    style={{ width: '200px' }}
                    options={['int', 'long', 'float', 'double', 'string', 'date', 'timestamp'].map(
                      item => ({
                        text: item,
                        value: item,
                      }),
                    )}
                  />
                ),
              },
              {
                key: 'fieldComment',
                header: '备注',
                rules: { validate: value => (value ? undefined : '请填写备注') },
                render: ({ field }) => <Input {...field} placeholder="请输入备注" />,
              },
            ]}
          />
        </Form.Item>

        <FieldsParse
          {...fieldsParse}
          onOk={data => {
            editableTable.current.setValue(data);
            setFieldsParse({ visible: false });
          }}
          onClose={() => setFieldsParse({ visible: false })}
        />
      </Form>
    );
  },
);

export default AccessForm;