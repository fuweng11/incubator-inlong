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
import ProductSelect from '@/components/ProductSelect';
import { SinkInfo } from '../common/SinkInfo';
import { sourceFields } from '../common/sourceFields';

const { I18n, FormField, TableColumn } = DataWithBackend;

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

export default class InnerHiveSink extends SinkInfo implements DataWithBackend {
  @FormField({
    type: ProductSelect,
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
  })
  @TableColumn()
  @I18n('meta.Group.Product')
  productId: string | number;

  @FormField({
    type: 'select',
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
  })
  @I18n('meta.Group.AppGroupName')
  appGroupName: string;

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
  })
  @I18n('meta.Sinks.InnerHive.DataNodeName')
  dataNodeName: string;

  @FormField({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.InnerHive.DbName')
  dbName: string;

  @FormField({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.InnerHive.TableName')
  tableName: string;

  @FormField({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    suffix: {
      type: 'select',
      name: 'partitionUnit',
      initialValue: 'D',
      rules: [{ required: true }],
      props: values => ({
        disabled: [110, 130].includes(values?.status),
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
      }),
    },
  })
  @I18n('meta.Sinks.InnerHive.PartitionType')
  partitionType: string;

  @FormField({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.InnerHive.PrimaryPartition')
  primaryPartition: string;

  @FormField({
    type: 'select',
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
  })
  @I18n('meta.Sinks.InnerHive.PartitionCreationStrategy')
  partitionCreationStrategy: string;

  @FormField({
    type: 'radio',
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
  })
  @I18n('meta.Sinks.InnerHive.FieldFormat')
  fileFormat: string;

  @FormField({
    type: 'radio',
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
  })
  @I18n('meta.Sinks.InnerHive.DataEncoding')
  dataEncoding: string;

  @FormField({
    type: 'select',
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
  })
  @I18n('meta.Sinks.InnerHive.DataSeparator')
  dataSeparator: string;

  @FormField({
    type: 'input',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.InnerHive.DefaultSelectors')
  defaultSelectors: string;

  @FormField({
    type: 'input',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.InnerHive.Responsible')
  virtualUser: string;

  @FormField({
    type: 'input',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    isPro: true,
  })
  @I18n('meta.Sinks.InnerHive.SecondaryPartition')
  secondaryPartition: string;

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
  @I18n('meta.Sinks.InnerHive.DataConsistency')
  dataConsistency: string;

  @FormField({
    type: 'input',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      placeholder: i18n.t('meta.Sinks.InnerHive.CheckHint'),
    }),
    isPro: true,
  })
  @I18n('meta.Sinks.InnerHive.CheckAbsolute')
  checkAbsolute: string;

  @FormField({
    type: 'input',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      placeholder: i18n.t('meta.Sinks.InnerHive.CheckHint'),
    }),
    isPro: true,
  })
  @I18n('meta.Sinks.InnerHive.CheckRelative')
  checkRelative: string;

  @FormField({
    type: EditableTable,
    props: values => ({
      size: 'small',
      columns: getFieldListColumns(values),
      canDelete: ![110, 130].includes(values?.status),
    }),
  })
  sinkFieldList: Record<string, unknown>[];

  @FormField({
    type: EditableTable,
    tooltip: i18n.t('meta.Sinks.Hive.PartitionFieldListHelp'),
    col: 24,
    props: {
      size: 'small',
      required: false,
      columns: [
        {
          title: i18n.t('meta.Sinks.Hive.FieldName'),
          dataIndex: 'fieldName',
          rules: [{ required: true }],
        },
        {
          title: i18n.t('meta.Sinks.Hive.FieldType'),
          dataIndex: 'fieldType',
          type: 'select',
          initialValue: 'string',
          props: {
            options: ['string', 'timestamp'].map(item => ({
              label: item,
              value: item,
            })),
          },
        },
        {
          title: i18n.t('meta.Sinks.Hive.FieldFormat'),
          dataIndex: 'fieldFormat',
          type: 'autocomplete',
          props: {
            options: ['MICROSECONDS', 'MILLISECONDS', 'SECONDS', 'SQL', 'ISO_8601'].map(item => ({
              label: item,
              value: item,
            })),
          },
          rules: [{ required: true }],
          visible: (text, record) => record.fieldType === 'timestamp',
        },
      ],
    },
  })
  @I18n('meta.Sinks.Hive.PartitionFieldList')
  partitionFieldList: Record<string, unknown>[];
}

const getFieldListColumns = sinkValues => {
  return [
    ...sourceFields,
    {
      title: `Hive${i18n.t('meta.Sinks.InnerHive.FieldName')}`,
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
      title: `Hive${i18n.t('meta.Sinks.InnerHive.FieldType')}`,
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
