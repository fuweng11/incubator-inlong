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

import { DataWithBackend } from '@/metas/DataWithBackend';
import i18n from '@/i18n';
import EditableTable from '@/components/EditableTable';
import { SinkInfo } from '../common/SinkInfo';
import { sourceFields } from '../common/sourceFields';

const { I18n, FormField, TableColumn } = DataWithBackend;

const innerClickHouseFieldTypes = [
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

export default class InnerClickHouseSink extends SinkInfo implements DataWithBackend {
  @FormField({
    type: 'select',
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
  })
  @TableColumn()
  @I18n('meta.Sinks.InnerClickHouse.DataNodeName')
  dataNodeName: string;

  @FormField({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @TableColumn()
  @I18n('meta.Sinks.InnerClickHouse.DbName')
  dbName: string;

  @FormField({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @TableColumn()
  @I18n('meta.Sinks.InnerClickHouse.TableName')
  tableName: string;

  @FormField({
    type: 'inputnumber',
    initialValue: 1,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 1,
    }),
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.InnerClickHouse.FlushIntervalUnit'),
  })
  @I18n('meta.Sinks.InnerClickHouse.FlushInterval')
  flushInterval: number;

  @FormField({
    type: 'inputnumber',
    initialValue: 1000,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 1,
    }),
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.InnerClickHouse.PackageSizeUnit'),
  })
  @I18n('meta.Sinks.InnerClickHouse.PackageSize')
  packageSize: number;

  @FormField({
    type: 'inputnumber',
    initialValue: 3,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      min: 1,
    }),
    rules: [{ required: true }],
    suffix: i18n.t('meta.Sinks.InnerClickHouse.RetryTimesUnit'),
  })
  @I18n('meta.Sinks.InnerClickHouse.RetryTimes')
  retryTime: number;

  @FormField({
    name: 'isDistribute',
    type: 'radio',
    initialValue: 0,
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: i18n.t('meta.Sinks.InnerClickHouse.Yes'),
          value: 1,
        },
        {
          label: i18n.t('meta.Sinks.InnerClickHouse.No'),
          value: 0,
        },
      ],
    }),
    rules: [{ required: true }],
  })
  @I18n('meta.Sinks.InnerClickHouse.IsDistributed')
  isDistribute: 0 | 1;

  @FormField({
    type: 'select',
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
  })
  @I18n('meta.Sinks.InnerClickHouse.PartitionStrategy')
  partitionStrategy: string;

  @FormField({
    type: 'select',
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
  })
  @I18n('meta.Sinks.InnerClickHouse.DataConsistency')
  dataConsistency: string;

  @FormField({
    type: EditableTable,
    props: values => ({
      size: 'small',
      columns: getFieldListColumns(values),
      editing: ![110, 130].includes(values?.status),
    }),
  })
  sinkFieldList: Record<string, unknown>[];
}

const getFieldListColumns = sinkValues => {
  return [
    ...sourceFields,
    {
      title: `ClickHouse${i18n.t('meta.Sinks.InnerClickHouse.FieldName')}`,
      dataIndex: 'fieldName',
      rules: [
        { required: true },
        {
          pattern: /^[a-zA-Z][a-zA-Z0-9_]*$/,
          message: i18n.t('meta.Sinks.InnerClickHouse.FieldNameRule'),
        },
      ],
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
    },
    {
      title: `ClickHouse${i18n.t('meta.Sinks.InnerClickHouse.FieldType')}`,
      dataIndex: 'fieldType',
      initialValue: innerClickHouseFieldTypes[0].value,
      type: 'select',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
        options: innerClickHouseFieldTypes,
      }),
      rules: [{ required: true }],
    },
    {
      title: `ClickHouse${i18n.t('meta.Sinks.InnerClickHouse.FieldDescription')}`,
      dataIndex: 'fieldComment',
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(sinkValues?.status as number) && !isNew,
      }),
    },
  ];
};
