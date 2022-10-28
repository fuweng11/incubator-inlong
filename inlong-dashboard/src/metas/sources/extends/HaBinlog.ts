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
import rulesPattern from '@/utils/pattern';
import { SourceInfo } from '../common/SourceInfo';

const { I18n, FormField, TableColumn } = DataWithBackend;

export default class PulsarSource extends SourceInfo implements DataWithBackend {
  @FormField({
    type: 'select',
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
  })
  @TableColumn()
  @I18n('meta.Sources.HaBinlog.Cluster')
  inlongClusterName: string;

  @FormField({
    type: 'select',
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
            pageNum: 1,
            pageSize: 20,
            type: 'MYSQL',
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
  })
  @TableColumn()
  @I18n('meta.Sources.HaBinlog.DataNode')
  dataNodeName: string;

  @FormField({
    type: 'input',
    rules: [{ required: true }],
  })
  @I18n('meta.Sources.HaBinlog.DBName')
  dbName: string;

  @FormField({
    type: 'input',
    rules: [{ required: true }],
    props: {
      placeholder: i18n.t('meta.Sources.HaBinlog.TableNameHint'),
    },
  })
  @I18n('meta.Sources.HaBinlog.TableName')
  tableName: string;

  @FormField({
    type: 'select',
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
  })
  @I18n('meta.Sources.HaBinlog.Encoding')
  charset: string;

  @FormField({
    type: 'radio',
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
  })
  @I18n('meta.Sources.HaBinlog.SkipDelete')
  skipDelete: number;

  @FormField({
    type: 'input',
  })
  @I18n('meta.Sources.HaBinlog.FileName')
  'startDumpPosition.entryPosition.journalName': string;

  @FormField({
    type: 'inputnumber',
    props: {
      min: 1,
      max: 1000000000,
      precision: 0,
    },
  })
  @I18n('meta.Sources.HaBinlog.FileLocation')
  'startDumpPosition.entryPosition.position': number;
}
