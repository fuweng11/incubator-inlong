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

import React, { forwardRef, useImperativeHandle, Ref } from 'react';
import { Form, Table, TableProps, Button, Icon, TableColumn } from '@tencent/tea-component';
import { useForm, useFieldArray, Controller, ControllerProps, FieldValues } from 'react-hook-form';
import styles from './index.module.less';

interface ColumnItemProps extends Omit<TableColumn, 'render'> {
  render: ({ field }) => React.ReactElement;
  // rules?: React.ComponentProps<typeof Controller>['rules'];
  rules?: ControllerProps['rules'];
}

interface EditableTableProps extends Omit<TableProps, 'columns'> {
  defaultValues: FieldValues[];
  columns: ColumnItemProps[];
  showRowIndex?: boolean;
}

export interface EditableTableRef {
  submit: () => Promise<FieldValues[]>;
  reset: Function;
  setValue: (data: FieldValues[]) => void;
}

const EditableTable = forwardRef((props: EditableTableProps, ref: Ref<EditableTableRef>) => {
  const { defaultValues, columns, showRowIndex = false } = props;

  const {
    control,
    formState,
    reset,
    handleSubmit,
    setValue: setV,
  } = useForm({
    defaultValues: {
      array: defaultValues,
    },
  });

  const { fields, append, remove } = useFieldArray({
    control,
    name: 'array',
  });

  const { errors } = formState;

  const submit = () => {
    return new Promise<FieldValues[]>((resolve, reject) => {
      handleSubmit(values => {
        resolve(values.array);
      }, reject)();
    });
  };

  const setValue = (values: FieldValues[]) => {
    setV('array', values);
  };

  useImperativeHandle(ref, () => ({
    submit,
    reset,
    setValue,
  }));

  return (
    <div>
      <Table
        compact
        bordered
        {...props}
        recordKey="id"
        records={fields}
        className={styles.editableTable}
        columns={[
          showRowIndex &&
            ({
              key: '__index',
              header: '序号',
              width: 80,
              render: (row, rowKey, index) => index + 1,
            } as TableColumn),
        ]
          .concat(
            columns.map(
              ({ render, rules, ...item }) =>
                ({
                  ...item,
                  render: (row, rowKey, index) => (
                    <Form.Item
                      align="middle"
                      status={errors.array?.[index]?.[item.key]?.message ? 'error' : undefined}
                      message={errors.array?.[index]?.[item.key]?.message}
                    >
                      <Controller
                        name={`array.${index}.${item.key}`}
                        control={control}
                        rules={rules as any}
                        render={render}
                      />
                    </Form.Item>
                  ),
                } as TableColumn),
            ),
          )
          .concat({
            key: '__actions',
            header: '操作',
            width: 100,
            render: (row, rowKey, index) => [
              <Button key="add" type="link" onClick={() => append({ ...defaultValues[0] })}>
                添加
              </Button>,
              <Button key="del" type="link" onClick={() => remove(index)}>
                删除
              </Button>,
            ],
          })
          .filter(Boolean)}
      />
      <div style={{ padding: '6px 10px', border: '1px solid #cfd5df', borderTop: 'none' }}>
        <Button
          type="link"
          style={{ display: 'flex', alignItems: 'center' }}
          onClick={() => append({ ...defaultValues[0] })}
        >
          <Icon type="plus" />
          添加字段
        </Button>
      </div>
    </div>
  );
});

export const addons = Table.addons;

export default EditableTable;
