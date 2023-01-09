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
import { RenderRow } from '@/metas/RenderRow';
import { RenderList } from '@/metas/RenderList';
import i18n from '@/i18n';
import EditableTable from '@/components/EditableTable';
import { SinkInfo } from '../common/SinkInfo';
import { sourceFields } from '../common/sourceFields';
import NodeSelect from '@/components/NodeSelect';

const { I18n } = DataWithBackend;
const { FieldDecorator } = RenderRow;

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

export default class InnerHiveSink
  extends SinkInfo
  implements DataWithBackend, RenderRow, RenderList
{
  @FieldDecorator({
    type: NodeSelect,
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      nodeType: 'INNER_HIVE',
    }),
  })
  @I18n('meta.Sinks.DataNodeName')
  dataNodeName: string;

  @FieldDecorator({
    type: 'select',
    rules: [{ required: true }],
    props: values => ({
      showSearch: true,
      requestTrigger: ['onOpen', 'onSearch'],
      options: {
        requestService: {
          url: '/sc/database/list',
          method: 'GET',
          params: {
            groupId: values.inlongGroupId,
            sinkType: 'INNER_HIVE',
            dataNodeName: values.dataNodeName,
            pageNum: 1,
            pageSize: 20,
          },
        },
        requestParams: {
          formatResult: result =>
            result?.map(item => ({
              label: item.database,
              value: item.database,
            })),
        },
      },
    }),
  })
  @I18n('meta.Sinks.InnerHive.DbName')
  dbName: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.InnerHive.TableName')
  tableName: string;

  @FieldDecorator({
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

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.InnerHive.PrimaryPartition')
  primaryPartition: string;

  @FieldDecorator({
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

  @FieldDecorator({
    type: 'radio',
    initialValue: 'OrcFile',
    rules: [{ required: true }],
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: 'OrcFile',
          value: 'OrcFile',
        },
        {
          label: 'TextFile',
          value: 'TextFile',
        },
        {
          label: 'Parquet',
          value: 'Parquet',
        },
        {
          label: 'SequenceFile',
          value: 'SequenceFile',
        },
        {
          label: 'MySequenceFile',
          value: 'MySequenceFile',
        },
      ],
    }),
  })
  @I18n('meta.Sinks.InnerHive.FieldFormat')
  fileFormat: string;

  @FieldDecorator({
    type: 'radio',
    initialValue: 'none',
    rules: [{ required: true }],
    visible: values => values.fileFormat === 'TextFile',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      options: [
        {
          label: 'none',
          value: 'none',
        },
        {
          label: 'gzip',
          value: 'gzip',
        },
        {
          label: 'lzo',
          value: 'lzo',
        },
      ],
    }),
  })
  @I18n('meta.Sinks.InnerHive.CompressionType')
  compressionType: string;

  @FieldDecorator({
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

  @FieldDecorator({
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

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.InnerHive.DefaultSelectors')
  defaultSelectors: string;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
  })
  @I18n('meta.Sinks.InnerHive.Responsible')
  virtualUser: string;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
    }),
    isPro: true,
  })
  @I18n('meta.Sinks.InnerHive.SecondaryPartition')
  secondaryPartition: string;

  @FieldDecorator({
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

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      placeholder: i18n.t('meta.Sinks.InnerHive.CheckHint'),
    }),
    isPro: true,
  })
  @I18n('meta.Sinks.InnerHive.CheckAbsolute')
  checkAbsolute: string;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: [110, 130].includes(values?.status),
      placeholder: i18n.t('meta.Sinks.InnerHive.CheckHint'),
    }),
    isPro: true,
  })
  @I18n('meta.Sinks.InnerHive.CheckRelative')
  checkRelative: string;

  @FieldDecorator({
    type: EditableTable,
    props: values => ({
      size: 'small',
      columns: getFieldListColumns(values),
      canDelete: ![110, 130].includes(values?.status),
    }),
  })
  sinkFieldList: Record<string, unknown>[];

  @FieldDecorator({
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
