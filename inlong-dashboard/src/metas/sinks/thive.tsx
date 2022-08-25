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

import React from 'react';
import {
  getColsFromFields,
  GetStorageColumnsType,
  GetStorageFormFieldsType,
} from '@/utils/metaData';
import { ColumnsType } from 'antd/es/table';
import EditableTable, { ColumnsItemProps } from '@/components/EditableTable';
import i18n from '@/i18n';
import { excludeObject } from '@/utils';
import { sourceFields } from './common/sourceFields';
import TextSwitch from '@/components/TextSwitch';
import request from '@/utils/request';

const thiveFieldTypes = [
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

const getForm: GetStorageFormFieldsType = (
  type,
  { currentValues, inlongGroupId, isEdit, dataType, form } = {} as any,
) => {
  const fileds = [
    {
      type: 'select',
      label: i18n.t('meta.Sinks.THive.AppGroupName'),
      name: 'appGroupName',
      rules: [{ required: true }],
      props: {
        options: {
          requestService: async () => {
            const groupData = await request(`group/get/${inlongGroupId}`);
            const appGroupData = await request({
              url: '/sc/appgroup/my',
              params: {
                productId: groupData.productId,
              },
            });
            return appGroupData;
          },
          requestParams: {
            formatResult: result =>
              result?.map(item => ({
                label: item,
                value: item,
              })),
          },
        },
      },
      _inTable: true,
    },
    {
      type: 'select',
      label: i18n.t('meta.Sinks.THive.DataNodeName'),
      name: 'dataNodeName',
      rules: [{ required: true }],
      props: {
        options: {
          requestService: async () => {
            const nodeData = await request({
              url: '/node/list',
            });
            return nodeData.list;
          },
          requestParams: {
            formatResult: result =>
              result?.map(item => ({
                label: item,
                value: item,
              })),
          },
        },
      },
      _inTable: true,
    },
    {
      type: 'input',
      label: i18n.t('meta.Sinks.THive.DbName'),
      name: 'dbName',
      rules: [{ required: true }],
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
      _inTable: true,
    },
    {
      type: 'input',
      label: i18n.t('meta.Sinks.THive.TableName'),
      name: 'tableName',
      rules: [{ required: true }],
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
      _inTable: true,
    },
    {
      type: 'input',
      label: i18n.t('meta.Sinks.THive.PartitionType'),
      name: 'partitionType',
      rules: [{ required: true }],
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
      _inTable: true,
      suffix: {
        type: 'select',
        name: 'partitionUnit',
        initialValue: 'D',
        rules: [{ required: true }],
        props: {
          disabled: isEdit && [110, 130].includes(currentValues?.status),
          options: [
            {
              label: i18n.t('meta.Sinks.THive.Day'),
              value: 'D',
            },
            {
              label: i18n.t('meta.Sinks.THive.Hour'),
              value: 'H',
            },
          ],
        },
        _inTable: true,
      },
    },
    {
      type: 'input',
      label: i18n.t('meta.Sinks.THive.PrimaryPartition'),
      name: 'primaryPartition',
      rules: [{ required: true }],
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
      _inTable: true,
    },
    {
      type: 'select',
      label: i18n.t('meta.Sinks.THive.PartitionCreationStrategy'),
      name: 'partitionCreationStrategy',
      initialValue: 'ARRIVED',
      rules: [{ required: true }],
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
        options: [
          {
            label: i18n.t('meta.Sinks.THive.DataArrives'),
            value: 'ARRIVED',
          },
          {
            label: i18n.t('meta.Sinks.THive.DataComplete'),
            value: 'COMPLETED',
          },
          {
            label: i18n.t('meta.Sinks.THive.DataVerified'),
            value: 'AGENT_COUNT_VERIFIED',
          },
          {
            label: i18n.t('meta.Sinks.THive.DataDistinct'),
            value: 'DATA_DISTINCT_VERIFIED',
          },
        ],
      },
      _inTable: true,
    },
    {
      type: 'radio',
      label: i18n.t('meta.Sinks.THive.FieldFormat'),
      name: 'fileFormat',
      initialValue: 'TextFile',
      rules: [{ required: true }],
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
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
      },
      _inTable: true,
    },
    {
      name: 'dataEncoding',
      type: 'radio',
      label: i18n.t('meta.Sinks.THive.DataEncoding'),
      initialValue: 'UTF-8',
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
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
      },
      rules: [{ required: true }],
    },
    {
      name: 'dataSeparator',
      type: 'select',
      label: i18n.t('meta.Sinks.THive.DataSeparator'),
      initialValue: '124',
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
        dropdownMatchSelectWidth: false,
        options: [
          {
            label: i18n.t('meta.Stream.VerticalLine'),
            value: '124',
          },
          {
            label: i18n.t('meta.Stream.Asterisk'),
            value: '42',
          },
          {
            label: i18n.t('meta.Stream.Comma'),
            value: '44',
          },
          {
            label: i18n.t('meta.Stream.Semicolon'),
            value: '59',
          },
          {
            label: i18n.t('meta.Stream.DoubleQuotes'),
            value: '34',
          },
          {
            label: i18n.t('meta.Stream.Space'),
            value: '32',
          },
          {
            label: i18n.t('meta.Stream.Tab'),
            value: '9',
          },
        ],
        useInput: true,
        useInputProps: {
          placeholder: 'ASCII',
          disabled: isEdit && [110, 130].includes(currentValues?.status),
        },
        style: { width: 100 },
      },
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
    { name: '_showHigher', type: <TextSwitch />, initialValue: false },
    {
      type: 'input',
      label: i18n.t('meta.Sinks.THive.SecondaryPartition'),
      name: 'secondaryPartition',
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
      },
      hidden: !currentValues?._showHigher,
    },
    {
      type: 'select',
      label: i18n.t('meta.Sinks.THive.DataConsistency'),
      name: 'dataConsistency',
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
      type: 'input',
      label: i18n.t('meta.Sinks.THive.CheckAbsolute'),
      name: 'checkAbsolute',
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
        placeholder: i18n.t('meta.Sinks.THive.CheckHint'),
      },
      hidden: !currentValues?._showHigher,
    },
    {
      type: 'input',
      label: i18n.t('meta.Sinks.THive.CheckRelative'),
      name: 'checkRelative',
      props: {
        disabled: isEdit && [110, 130].includes(currentValues?.status),
        placeholder: i18n.t('meta.Sinks.THive.CheckHint'),
      },
      hidden: !currentValues?._showHigher,
    },
    {
      type: (
        <EditableTable
          size="small"
          columns={getFieldListColumns(dataType, currentValues)}
          canDelete={(record, idx, isNew) => !isEdit || isNew}
        />
      ),
      name: 'sinkFieldList',
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
      title: `THIVE${i18n.t('meta.Sinks.THive.FieldName')}`,
      dataIndex: 'fieldName',
      initialValue: '',
      rules: [
        { required: true },
        {
          pattern: /^[a-z][0-9a-z_]*$/,
          message: i18n.t('meta.Sinks.THive.FieldNameRule'),
        },
      ],
      props: (text, record, idx, isNew) => ({
        disabled: [110, 130].includes(currentValues?.status as number) && !isNew,
      }),
    },
    {
      title: `THIVE${i18n.t('meta.Sinks.THive.FieldType')}`,
      dataIndex: 'fieldType',
      initialValue: thiveFieldTypes[0].value,
      type: 'select',
      props: (text, record, idx, isNew) => ({
        options: thiveFieldTypes,
        disabled: [110, 130].includes(currentValues?.status as number) && !isNew,
      }),
      rules: [{ required: true }],
    },
    {
      title: i18n.t('meta.Sinks.THive.FieldDescription'),
      dataIndex: 'fieldComment',
      initialValue: '',
    },
  ] as ColumnsItemProps[];
};

const tableColumns = getForm('col') as ColumnsType;

export const thive = {
  getForm,
  getFieldListColumns,
  tableColumns,
};
