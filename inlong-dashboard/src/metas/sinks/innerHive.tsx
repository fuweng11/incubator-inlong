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

import i18n from '@/i18n';
import type { FieldItemType } from '@/metas/common';
import EditableTable from '@/components/EditableTable';
import ProductSelect from '@/components/ProductSelect';
import UserSelect from '@/components/UserSelect';
import { sourceFields } from './common/sourceFields';
import React from 'react';

const innerHiveFieldTypes = [
  'string',
  'varchar',
  'char',
  'tinyint',
  'smallint',
  'int',
  'bigint',
  'float',
  'double',
  'decimal',
  'numeric',
  'boolean',
  'binary',
  'timestamp',
  'date',
].map(item => ({
  label: item,
  value: item,
}));

export const innerHive: FieldItemType[] = [
  {
    type: ProductSelect,
    label: i18n.t('meta.Group.Product'),
    name: 'productId',
    extraNames: ['productName'],
    rules: [{ required: true }],
    props: values => ({
      asyncValueLabel: values.productName,
      disabled: [110, 130].includes(values?.status),
      onChange: (value, record) => ({
        appGroupName: undefined,
        productName: record.name,
      }),
    }),
  },
  {
    type: 'select',
    label: i18n.t('meta.Group.AppGroupName'),
    name: 'appGroupName',
    rules: [{ required: true }],
    props: values => ({
      allowClear: true,
      disabled: [110, 130].includes(values?.status),
      options: {
        requestService: {
          url: '/sc/appgroup/my',
          params: {
            productId: values.productId,
          },
        },
        requestParams: {
          formatResult: result =>
            result?.map(item => ({
              label: item,
              value: item,
            })),
        },
      },
    }),
  },
  {
    type: 'select',
    label: i18n.t('meta.Sinks.InnerHive.DataNodeName'),
    name: 'dataNodeName',
    rules: [{ required: true }],
    props: values => ({
      showSearch: true,
      disabled: [110, 130].includes(values?.status),
      options: {
        requestService: {
          url: '/node/list',
          method: 'POST',
          data: {
            type: 'INNER_HIVE',
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
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.InnerHive.DbName'),
    name: 'dbName',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.InnerHive.TableName'),
    name: 'tableName',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.InnerHive.PartitionType'),
    name: 'partitionType',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    suffix: {
      type: 'select',
      name: 'partitionUnit',
      initialValue: 'D',
      rules: [{ required: true }],
      props: {
        options: [
          {
            label: i18n.t('meta.Sinks.InnerHive.Day'),
            value: 'D',
          },
          {
            label: i18n.t('meta.Sinks.InnerHive.Hour'),
            value: 'H',
          },
        ],
      },
      _renderTable: true,
    },
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.InnerHive.PrimaryPartition'),
    name: 'primaryPartition',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    type: 'select',
    label: i18n.t('meta.Sinks.InnerHive.PartitionCreationStrategy'),
    name: 'partitionCreationStrategy',
    initialValue: 'ARRIVED',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: i18n.t('meta.Sinks.InnerHive.DataArrives'),
          value: 'ARRIVED',
        },
        {
          label: i18n.t('meta.Sinks.InnerHive.DataComplete'),
          value: 'COMPLETED',
        },
        {
          label: i18n.t('meta.Sinks.InnerHive.DataVerified'),
          value: 'AGENT_COUNT_VERIFIED',
        },
        {
          label: i18n.t('meta.Sinks.InnerHive.DataDistinct'),
          value: 'DATA_DISTINCT_VERIFIED',
        },
      ],
    }),
    _renderTable: true,
  },
  {
    type: 'radio',
    label: i18n.t('meta.Sinks.InnerHive.FieldFormat'),
    name: 'fileFormat',
    initialValue: 'TextFile',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: 'TextFile',
          value: 'TextFile',
        },
        {
          label: 'OrcFile',
          value: 'OrcFile',
        },
        {
          label: 'Parquet',
          value: 'Parquet',
        },
      ],
    }),
    _renderTable: true,
  },
  {
    name: 'dataEncoding',
    type: 'radio',
    label: i18n.t('meta.Sinks.InnerHive.DataEncoding'),
    initialValue: 'UTF-8',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: 'UTF-8',
          value: 'UTF-8',
        },
        {
          label: 'GBK',
          value: 'GBK',
        },
      ],
    }),
    rules: [{ required: true }],
  },
  {
    name: 'dataSeparator',
    type: 'select',
    label: i18n.t('meta.Sinks.InnerHive.DataSeparator'),
    initialValue: '124',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      dropdownMatchSelectWidth: false,
      options: [
        {
          label: i18n.t('meta.Sinks.Hive.DataSeparator.VerticalLine'),
          value: '124',
        },
        {
          label: i18n.t('meta.Sinks.Hive.DataSeparator.Comma'),
          value: '44',
        },
        {
          label: i18n.t('meta.Sinks.Hive.DataSeparator.DoubleQuotes'),
          value: '34',
        },
        {
          label: i18n.t('meta.Sinks.Hive.DataSeparator.Asterisk'),
          value: '42',
        },
        {
          label: i18n.t('meta.Sinks.Hive.DataSeparator.Space'),
          value: '32',
        },
        {
          label: i18n.t('meta.Sinks.Hive.DataSeparator.Semicolon'),
          value: '59',
        },
      ],
      useInput: true,
      useInputProps: {
        placeholder: 'ASCII',
        disabled: [110, 130].includes(values?.status),
      },
      style: { width: 100 },
    }),
    rules: [
      {
        required: true,
        type: 'number',
        transform: val => +val,
        min: 0,
        max: 127,
      } as any,
    ],
  },
  {
    type: UserSelect,
    label: i18n.t('meta.Sinks.InnerHive.DefaultSelectors'),
    name: 'defaultSelectors',
    props: {
      mode: 'multiple',
      currentUserClosable: false,
    },
  },
  {
    type: <UserSelect />,
    label: i18n.t('meta.Sinks.InnerHive.Responsible'),
    name: 'virtualUser',
    props: {
      currentUserClosable: false,
    },
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.InnerHive.SecondaryPartition'),
    name: 'secondaryPartition',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    isPro: true,
  },
  {
    type: 'select',
    label: i18n.t('meta.Sinks.InnerHive.DataConsistency'),
    name: 'dataConsistency',
    initialValue: 'EXACTLY_ONCE',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
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
    }),
    isPro: true,
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.InnerHive.CheckAbsolute'),
    name: 'checkAbsolute',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      placeholder: i18n.t('meta.Sinks.InnerHive.CheckHint'),
    }),
    isPro: true,
  },
  {
    type: 'input',
    label: i18n.t('meta.Sinks.InnerHive.CheckRelative'),
    name: 'checkRelative',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      placeholder: i18n.t('meta.Sinks.InnerHive.CheckHint'),
    }),
    isPro: true,
  },
  {
    name: 'sinkFieldList',
    type: EditableTable,
    props: values => ({
      size: 'small',
      columns: getFieldListColumns(values),
      canDelete: ![110, 130].includes(values?.status),
    }),
  },
];

const getFieldListColumns = sinkValues => {
  return [
    ...sourceFields,
    {
      title: `HIVE${i18n.t('meta.Sinks.InnerHive.FieldName')}`,
      dataIndex: 'fieldName',
      initialValue: '',
      rules: [
        { required: true },
        {
          pattern: /^[a-z][0-9a-z_]*$/,
          message: i18n.t('meta.Sinks.InnerHive.FieldNameRule'),
        },
      ],
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
    },
    {
      title: `HIVE${i18n.t('meta.Sinks.InnerHive.FieldType')}`,
      dataIndex: 'fieldType',
      initialValue: innerHiveFieldTypes[0].value,
      type: 'select',
      props: (text, record, idx, isNew) => ({
        options: innerHiveFieldTypes,
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
      rules: [{ required: true }],
    },
    {
      title: i18n.t('meta.Sinks.InnerHive.FieldDescription'),
      dataIndex: 'fieldComment',
      initialValue: '',
    },
  ];
};
