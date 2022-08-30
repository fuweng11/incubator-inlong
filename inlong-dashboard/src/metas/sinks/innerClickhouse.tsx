/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  getColsFromFields,
  GetStorageColumnsType,
  GetStorageFormFieldsType,
} from '@/utils/metaData';
import i18n from '@/i18n';
import { ColumnsType } from 'antd/es/table';
import EditableTable, { ColumnsItemProps } from '@/components/EditableTable';
import { excludeObject } from '@/utils';
import { sourceFields } from './common/sourceFields';
import TextSwitch from '@/components/TextSwitch';
import React from 'react';

const innerClickhouseFieldTypes = [
  'String',
  'boolean',
  'byte',
  'short',
  'int',
  'long',
  'float',
  'double',
  'decimal',
].map(item => ({
  label: item,
  value: item,
}));

const getForm: GetStorageFormFieldsType = (
  type: 'form' | 'col' = 'form',
  { currentValues, inlongGroupId, isEdit, dataType } = {} as any,
) => {
  const fileds = [
    {
      name: 'dbName',
      type: 'input',
      label: i18n.t('meta.Sinks.InnerClickhouse.DbName'),
      rules: [{ required: true }],
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
      _inTable: true,
    },
    {
      name: 'tableName',
      type: 'input',
      label: i18n.t('meta.Sinks.InnerClickhouse.TableName'),
      rules: [{ required: true }],
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
      _inTable: true,
    },
    {
      type: 'select',
      label: i18n.t('meta.Sinks.InnerClickhouse.DataNodeName'),
      name: 'dataNodeName',
      rules: [{ required: true }],
      props: values => ({
        showSearch: true,
        options: {
          requestService: {
            url: '/node/list',
            method: 'POST',
            data: {
              type: 'INNER_CK',
              pageNum: 1,
              pageSize: 20,
            },
          },
          requestParams: {
            formatResult: result =>
              result?.list?.map(item => ({
                label: item.name,
                value: item.name,
              })),
          },
        },
      }),
      _inTable: true,
    },
    {
      name: 'flushInterval',
      type: 'inputnumber',
      label: i18n.t('meta.Sinks.InnerClickhouse.FlushInterval'),
      initialValue: 1,
      props: {
        min: 1,
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
      rules: [{ required: true }],
      suffix: i18n.t('meta.Sinks.InnerClickhouse.FlushIntervalUnit'),
    },
    {
      name: 'packageSize',
      type: 'inputnumber',
      label: i18n.t('meta.Sinks.InnerClickhouse.PackageSize'),
      initialValue: 1000,
      props: {
        min: 1,
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
      rules: [{ required: true }],
      suffix: i18n.t('meta.Sinks.InnerClickhouse.PackageSizeUnit'),
    },
    {
      name: 'retryTime',
      type: 'inputnumber',
      label: i18n.t('meta.Sinks.InnerClickhouse.RetryTimes'),
      initialValue: 3,
      props: {
        min: 1,
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
      rules: [{ required: true }],
      suffix: i18n.t('meta.Sinks.InnerClickhouse.RetryTimesUnit'),
    },
    {
      name: 'isDistributed',
      type: 'radio',
      label: i18n.t('meta.Sinks.InnerClickhouse.IsDistributed'),
      initialValue: 0,
      props: {
        options: [
          {
            label: i18n.t('meta.Sinks.InnerClickhouse.Yes'),
            value: 1,
          },
          {
            label: i18n.t('meta.Sinks.InnerClickhouse.No'),
            value: 0,
          },
        ],
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
      rules: [{ required: true }],
    },
    {
      name: 'partitionStrategy',
      type: 'select',
      label: i18n.t('meta.Sinks.InnerClickhouse.PartitionStrategy'),
      initialValue: 'BALANCE',
      rules: [{ required: true }],
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
        options: [
          {
            label: 'BALANCE',
            value: 'BALANCE',
          },
          {
            label: 'RANDOM',
            value: 'RANDOM',
          },
          {
            label: 'HASH',
            value: 'HASH',
          },
        ],
      },
      visible: values => values.isDistributed,
      _inTable: true,
    },
    { name: '_showHigher', type: <TextSwitch />, initialValue: false },
    {
      name: 'dataConsistency',
      type: 'select',
      label: i18n.t('meta.Sinks.InnerClickhouse.DataConsistency'),
      initialValue: 'EXACTLY_ONCE',
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
        options: [
          {
            label: 'EXACTLY_ONCE',
            value: 'EXACTLY_ONCE',
          },
          {
            label: 'AT_LEAST_ONCE',
            value: 'AT_LEAST_ONCE',
          },
        ],
      },
      hidden: !currentValues?._showHigher,
    },
    {
      name: 'sinkFieldList',
      type: EditableTable,
      props: {
        size: 'small',
        editing: ![110, 130].includes(currentValues?.status),
        columns: getFieldListColumns(dataType, currentValues),
      },
    },
  ];

  return type === 'col'
    ? getColsFromFields(fileds)
    : fileds.map(item => excludeObject(['_inTable'], item));
};

const getFieldListColumns: GetStorageColumnsType = (dataType, currentValues) => {
  return [
    ...sourceFields,
    {
      title: `ClickHouse${i18n.t('meta.Sinks.InnerClickhouse.FieldName')}`,
      dataIndex: 'fieldName',
      rules: [
        { required: true },
        {
          pattern: /^[a-zA-Z][a-zA-Z0-9_]*$/,
          message: i18n.t('meta.Sinks.InnerClickhouse.FieldNameRule'),
        },
      ],
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(currentValues?.status as number) && !isNew,
      }),
    },
    {
      title: `ClickHouse${i18n.t('meta.Sinks.InnerClickhouse.FieldType')}`,
      dataIndex: 'fieldType',
      initialValue: innerClickhouseFieldTypes[0].value,
      type: 'select',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(currentValues?.status as number) && !isNew,
        options: innerClickhouseFieldTypes,
      }),
      rules: [{ required: true }],
    },
    {
      title: `ClickHouse${i18n.t('meta.Sinks.InnerClickhouse.FieldDescription')}`,
      dataIndex: 'fieldComment',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(currentValues?.status as number) && !isNew,
      }),
    },
  ] as ColumnsItemProps[];
};

const tableColumns = getForm('col') as ColumnsType;

export const innerClickhouse = {
  getForm,
  getFieldListColumns,
  tableColumns,
};
