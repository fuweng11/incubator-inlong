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
import { Form, Table, TableProps, Button, Icon, TableColumn } from '@tencent/tea-component';
import { useForm, useFieldArray, Controller, ControllerProps, FieldValues } from 'react-hook-form';
import styles from './index.module.less';

interface ColumnItemProps extends Omit<TableColumn, 'render'> {
  render: ({ field }) => React.ReactElement;
  // rules?: React.ComponentProps<typeof Controller>['rules'];
  rules?: ControllerProps['rules'];
}

export interface EditableTableProps extends Omit<TableProps, 'columns'> {
  defaultValues: FieldValues[];
  columns: ColumnItemProps[];
  showRowIndex?: boolean;
  showRowCreate?: boolean | ((rowKey: string, rowIndex: number) => boolean);
  showRowRemove?: boolean | ((rowKey: string, rowIndex: number) => boolean);
  showRowUpdate?: boolean | ((rowKey: string, rowIndex: number) => boolean);
  onChange?: <T>(values: T) => void;
}

export interface EditableTableRef {
  submit: () => Promise<FieldValues[]>;
  reset: Function;
  setValue: (data: FieldValues[]) => void;
}

const EditableTable = forwardRef((props: EditableTableProps, ref: Ref<EditableTableRef>) => {
  const {
    defaultValues,
    columns,
    showRowIndex = false,
    showRowCreate: rowC = true,
    showRowRemove: rowR = true,
    showRowUpdate: rowU = true,
    onChange,
  } = props;

  const {
    control,
    formState,
    reset,
    handleSubmit,
    setValue: setV,
    watch,
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

  useEffect(() => {
    const { unsubscribe } = watch(values => onChange?.(values));
    return () => unsubscribe();
  }, [watch, onChange]);

  const btnStyle: React.CSSProperties = { zIndex: 9 };

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
                        render={({ field }) => {
                          (field as any).disabled =
                            typeof rowU === 'function' ? rowU(rowKey, index) : !rowU;
                          return render({ field });
                        }}
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
              (typeof rowC === 'function' ? rowC(rowKey, index) : rowC) && (
                <Button
                  style={btnStyle}
                  key="add"
                  type="link"
                  onClick={() => append({ ...defaultValues[0] })}
                >
                  添加
                </Button>
              ),
              (typeof rowR === 'function' ? rowR(rowKey, index) : rowR) && (
                <Button style={btnStyle} key="del" type="link" onClick={() => remove(index)}>
                  删除
                </Button>
              ),
            ],
          })
          .filter(Boolean)}
      />

      <div style={{ padding: '6px 10px', border: '1px solid #cfd5df', borderTop: 'none' }}>
        <Button
          type="link"
          style={{ display: 'flex', alignItems: 'center', zIndex: 9 }}
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
