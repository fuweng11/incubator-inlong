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
import omit from 'lodash/omit';
import {
  accessTypeMap,
  AccessTypeEnum,
  peakRateMap,
  PeakRateEnum,
  encodeTypeMap,
  EncodeTypeEnum,
  dataSeparatorMap,
  DataSeparatorEnum,
  StatusEnum,
} from '@/@tencent/enums/stream';
import FieldsParse from '@/@tencent/components/FieldsParse';
import EditableTable, { addons, EditableTableRef } from '@/@tencent/components/EditableTable';
import File from './File';
import Db from './Db';

export interface AccessFormRef {
  submit: () => Promise<Record<string, any>>;
  reset: Function;
}

export interface AccessFormProps extends FormProps {
  isUpdate?: boolean;
  savedData?: Record<string, any>;
  onChange?: <T>(values: T) => void;
}

const AccessForm = forwardRef(
  ({ isUpdate, savedData, onChange, ...props }: AccessFormProps, ref: Ref<AccessFormRef>) => {
    const editableTable = useRef<EditableTableRef>();

    const form = useForm({
      mode: 'onChange',
      defaultValues: isUpdate
        ? undefined
        : {
            accessModel: AccessTypeEnum.SDK,
            peakRate: PeakRateEnum.L2,
            peakTotalSize: 20,
            msgMaxLength: 20,
            encodeType: EncodeTypeEnum.UTF8,
            dataSeparator: DataSeparatorEnum.Comma,
          },
    });

    const { control, formState, reset, handleSubmit, watch } = form;

    const { errors } = formState;

    useEffect(() => {
      if (isUpdate && savedData) {
        const values = omit(savedData, [
          'name',
          'dataLevel',
          'remark',
          'sinkInnerHive',
          'subscribeTHive',
        ]);
        reset(values);
        editableTable.current.setValue(savedData.fieldsData);
      }
    }, [isUpdate, reset, savedData]);

    const watchAccessModel = watch('accessModel', AccessTypeEnum.SDK);

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
      const values = { ...v1, fieldsData: v2 };
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
          status={errors.accessModel?.message ? 'error' : undefined}
          message={errors.accessModel?.message}
        >
          <Controller
            name="accessModel"
            control={control}
            rules={{ required: '请填写接入方式' }}
            render={({ field }) => (
              <Segment
                {...(field as any)}
                options={Array.from(accessTypeMap).map(([key, ctx]) => ({
                  value: key,
                  text: ctx,
                  disabled: isUpdate && key !== field.value,
                }))}
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
          status={errors.peakRate?.message ? 'error' : undefined}
          message={errors.peakRate?.message}
        >
          <Controller
            name="peakRate"
            control={control}
            rules={{ required: '请填写单日峰值' }}
            render={({ field }) => (
              <Select
                {...(field as any)}
                appearance="button"
                options={Array.from(peakRateMap).map(([key, ctx]) => ({
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
          status={errors.peakTotalSize?.message ? 'error' : undefined}
          message={errors.peakTotalSize?.message}
        >
          <Controller
            name="peakTotalSize"
            control={control}
            rules={{ required: '请填写单日最大接入量' }}
            render={({ field }) => <InputNumber {...field} min={0} />}
          />
        </Form.Item>

        <Form.Item
          label="单条数据最大值"
          align="middle"
          suffix="Byte"
          required
          status={errors.msgMaxLength?.message ? 'error' : undefined}
          message={errors.msgMaxLength?.message}
        >
          <Controller
            name="msgMaxLength"
            control={control}
            rules={{ required: '请填写单条数据最大值' }}
            render={({ field }) => <InputNumber {...field} min={0} />}
          />
        </Form.Item>

        {(() => {
          const compMap = {
            [AccessTypeEnum.FILE]: File,
            [AccessTypeEnum.DB]: Db,
          };
          const Comp = compMap[watchAccessModel];
          return Comp ? (
            <>
              <Form.Title>数据源信息</Form.Title>
              <Comp form={form} />
            </>
          ) : null;
        })()}

        <Form.Title>数据格式</Form.Title>

        <Form.Item
          label="编码类型"
          align="middle"
          required
          status={errors.encodeType?.message ? 'error' : undefined}
          message={errors.encodeType?.message}
        >
          <Controller
            name="encodeType"
            control={control}
            rules={{ required: '请填写编码类型' }}
            render={({ field }) => (
              <Segment
                {...field}
                options={Array.from(encodeTypeMap).map(([key, ctx]) => ({
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
            rules={{ required: '请填写分隔符' }}
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
            showRowRemove={
              isUpdate ? (key, index) => index > savedData?.fieldsData?.length - 1 : true
            }
            showRowUpdate={
              // 没上线的接入都可以添加和修改字段
              isUpdate && savedData?.status === StatusEnum.Success
                ? (key, index) => index < savedData?.fieldsData?.length
                : true
            }
            addons={[
              addons.scrollable({
                maxHeight: 310,
              }),
            ]}
            defaultValues={[
              {
                fieldName: '',
                fieldType: '',
                remark: '',
              },
            ]}
            columns={[
              {
                key: 'fieldName',
                header: '字段名',
                rules: {
                  required: '请填写字段名',
                  pattern: {
                    value: /^\w+$/,
                    message: '仅支持英文字母、数字、下划线',
                  },
                },
                render: ({ field }) => <Input {...field} placeholder="请输入字段名" />,
              },
              {
                key: 'fieldType',
                header: '类型',
                rules: { required: '请填写类型' },
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
                key: 'remark',
                header: '备注',
                rules: { required: '请填写备注' },
                render: ({ field }) => <Input {...field} placeholder="请输入备注" />,
              },
            ]}
          />
        </Form.Item>

        <FieldsParse
          {...fieldsParse}
          onOk={data => {
            editableTable.current.setValue(
              isUpdate && savedData?.fieldsData ? savedData.fieldsData.concat(data) : data,
            );
            setFieldsParse({ visible: false });
          }}
          onClose={() => setFieldsParse({ visible: false })}
        />
      </Form>
    );
  },
);

export default AccessForm;
