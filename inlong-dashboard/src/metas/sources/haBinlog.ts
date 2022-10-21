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

import i18n from '@/i18n';
import rulesPattern from '@/utils/pattern';
import type { FieldItemType } from '@/metas/common';

export const haBinlog: FieldItemType[] = [
  {
    name: 'agentIp',
    type: 'input',
    label: 'Agent IP',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
    }),
  },
  {
    type: 'select',
    label: i18n.t('meta.Sources.HaBinlog.Cluster'),
    name: 'inlongClusterName',
    rules: [{ required: true }],
    props: values => ({
      showSearch: true,
      disabled: values?.status === 101,
      options: {
        requestTrigger: ['onOpen', 'onSearch'],
        requestService: keyword => ({
          url: '/cluster/list',
          method: 'POST',
          data: {
            keyword,
            pageNum: 1,
            pageSize: 20,
            type: 'AGENT',
          },
        }),
        requestParams: {
          formatResult: result =>
            result?.list?.map(item => ({
              ...item,
              label: item.name,
              value: item.name,
            })),
        },
      },
    }),
  },
  {
    type: 'select',
    label: i18n.t('meta.Sources.HaBinlog.DataNode'),
    name: 'dataNodeName',
    rules: [{ required: true }],
    props: values => ({
      showSearch: true,
      disabled: values?.status === 101,
      options: {
        requestTrigger: ['onOpen', 'onSearch'],
        requestService: keyword => ({
          url: '/node/list',
          method: 'POST',
          data: {
            keyword,
            pageNum: 1,
            pageSize: 20,
          },
        }),
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
    name: 'serializationType',
    type: 'radio',
    label: i18n.t('meta.Sources.HaBinlog.SerializationType'),
    initialValue: 'CSV',
    rules: [{ required: true }],
    props: values => ({
      disabled: values?.status === 101,
      options: [
        {
          label: 'CSV',
          value: 'CSV',
        },
        {
          label: 'JSON',
          value: 'JSON',
        },
        {
          label: 'CANAL',
          value: 'CANAL',
        },
        {
          label: 'AVRO',
          value: 'AVRO',
        },
      ],
    }),
  },
  {
    name: 'snapshot',
    type: 'input',
    label: 'Snapshot',
    rules: [{ required: true }],
  },
  {
    name: 'dbName',
    type: 'input',
    label: i18n.t('meta.Sources.HaBinlog.DBName'),
    rules: [{ required: true }],
  },
  {
    name: 'tableName',
    type: 'input',
    label: i18n.t('meta.Sources.HaBinlog.TableName'),
    rules: [{ required: true }],
    props: {
      placeholder: i18n.t('meta.Sources.HaBinlog.TableNameHint'),
    },
  },
  {
    name: 'charset',
    type: 'select',
    label: i18n.t('meta.Sources.HaBinlog.Encoding'),
    rules: [{ required: true }],
    initialValue: 'UTF-8',
    props: {
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
  },
  {
    name: 'skipDelete',
    type: 'radio',
    label: i18n.t('meta.Sources.HaBinlog.SkipDelete'),
    rules: [{ required: true }],
    initialValue: 1,
    props: {
      options: [
        {
          label: i18n.t('basic.Yes'),
          value: 1,
        },
        {
          label: i18n.t('basic.No'),
          value: 0,
        },
      ],
    },
  },
  {
    name: 'startDumpPosition.logIdentity.sourceIp',
    type: 'input',
    label: i18n.t('meta.Sources.HaBinlog.IP'),
    rules: [
      {
        pattern: rulesPattern.ip,
        message: i18n.t('meta.Sources.HaBinlog.IPMessage'),
      },
    ],
  },
  {
    name: 'startDumpPosition.logIdentity.sourcePort',
    type: 'inputnumber',
    label: i18n.t('meta.Sources.HaBinlog.Port'),
    rules: [
      {
        pattern: rulesPattern.port,
        message: i18n.t('meta.Sources.HaBinlog.PortMessage'),
      },
    ],
  },
  {
    name: 'startDumpPosition.entryPosition.journalName',
    type: 'input',
    label: i18n.t('meta.Sources.HaBinlog.FileName'),
  },
  {
    name: 'startDumpPosition.entryPosition.position',
    type: 'inputnumber',
    label: i18n.t('meta.Sources.HaBinlog.FileLocation'),
    props: {
      min: 1,
      max: 1000000000,
      precision: 0,
    },
  },
];
