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
import { sourceFields } from './common/sourceFields';

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

export const innerClickhouse: FieldItemType[] = [
  {
    name: 'dbName',
    type: 'input',
    label: i18n.t('meta.Sinks.InnerClickhouse.DbName'),
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    name: 'tableName',
    type: 'input',
    label: i18n.t('meta.Sinks.InnerClickhouse.TableName'),
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    _renderTable: true,
  },
  {
    type: 'select',
    label: i18n.t('meta.Sinks.InnerClickhouse.DataNodeName'),
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
    _renderTable: true,
  },
  {
    name: 'flushInterval',
    type: 'inputnumber',
    label: i18n.t('meta.Sinks.InnerClickhouse.FlushInterval'),
    initialValue: 1,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 1,
    }),
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.InnerClickhouse.FlushIntervalUnit'),
  },
  {
    name: 'packageSize',
    type: 'inputnumber',
    label: i18n.t('meta.Sinks.InnerClickhouse.PackageSize'),
    initialValue: 1000,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 1,
    }),
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.InnerClickhouse.PackageSizeUnit'),
  },
  {
    name: 'retryTime',
    type: 'inputnumber',
    label: i18n.t('meta.Sinks.InnerClickhouse.RetryTimes'),
    initialValue: 3,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 1,
    }),
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.InnerClickhouse.RetryTimesUnit'),
  },
  {
    name: 'isDistribute',
    type: 'radio',
    label: i18n.t('meta.Sinks.InnerClickhouse.IsDistributed'),
    initialValue: 0,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
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
    }),
    rules: [{ required: true }],
  },
  {
    name: 'partitionStrategy',
    type: 'select',
    label: i18n.t('meta.Sinks.InnerClickhouse.PartitionStrategy'),
    initialValue: 'BALANCE',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
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
    }),
    visible: values => values.isDistributed,
    _renderTable: true,
  },
  {
    name: 'dataConsistency',
    type: 'select',
    label: i18n.t('meta.Sinks.InnerClickhouse.DataConsistency'),
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
    name: 'sinkFieldList',
    type: EditableTable,
    props: values => ({
      size: 'small',
      columns: getFieldListColumns(values),
      editing: ![110, 130].includes(values?.status),
    }),
  },
];

const getFieldListColumns = sinkValues => {
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
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
    },
    {
      title: `ClickHouse${i18n.t('meta.Sinks.InnerClickhouse.FieldType')}`,
      dataIndex: 'fieldType',
      initialValue: innerClickhouseFieldTypes[0].value,
      type: 'select',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
        options: innerClickhouseFieldTypes,
      }),
      rules: [{ required: true }],
    },
    {
      title: `ClickHouse${i18n.t('meta.Sinks.InnerClickhouse.FieldDescription')}`,
      dataIndex: 'fieldComment',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
    },
  ];
};
