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

import { DataWithBackend } from '@/plugins/DataWithBackend';
import { RenderRow } from '@/plugins/RenderRow';
import { RenderList } from '@/plugins/RenderList';
import i18n from '@/i18n';
import { SourceInfo } from '../common/SourceInfo';

const { I18n } = DataWithBackend;
const { FieldDecorator } = RenderRow;
const { ColumnDecorator } = RenderList;

export default class PulsarSource
  extends SourceInfo
  implements DataWithBackend, RenderRow, RenderList
{
  @FieldDecorator({
    type: 'select',
    rules: [{ required: true }],
    props: values => ({
      showSearch: true,
      disabled: [200, 201, 202, 204, 205, 300, 301, 302, 304, 305].includes(values?.status),
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
  @ColumnDecorator()
  @I18n('meta.Sources.HaBinlog.Cluster')
  inlongClusterName: string;

  @FieldDecorator({
    type: 'select',
    rules: [{ required: true }],
    props: values => ({
      showSearch: true,
      disabled: [200, 201, 202, 204, 205, 300, 301, 302, 304, 305].includes(values?.status),
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
  @ColumnDecorator()
  @I18n('meta.Sources.HaBinlog.DataNode')
  dataNodeName: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [200, 201, 202, 204, 205, 300, 301, 302, 304, 305].includes(values?.status),
    }),
  })
  @I18n('meta.Sources.HaBinlog.DBName')
  dbName: string;

  @FieldDecorator({
    type: 'input',
    rules: [{ required: true }],
    props: values => ({
      disabled: [200, 201, 202, 204, 205, 300, 301, 302, 304, 305].includes(values?.status),
    }),
  })
  @I18n('meta.Sources.HaBinlog.TableName')
  tableName: string;

  @FieldDecorator({
    type: 'select',
    rules: [{ required: true }],
    initialValue: 'UTF-8',
    props: values => ({
      disabled: [200, 201, 202, 204, 205, 300, 301, 302, 304, 305].includes(values?.status),
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
  })
  @I18n('meta.Sources.HaBinlog.Encoding')
  charset: string;

  @FieldDecorator({
    type: 'radio',
    rules: [{ required: true }],
    initialValue: 1,
    props: values => ({
      disabled: [200, 201, 202, 204, 205, 300, 301, 302, 304, 305].includes(values?.status),
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
    }),
  })
  @I18n('meta.Sources.HaBinlog.SkipDelete')
  skipDelete: number;

  @FieldDecorator({
    type: 'input',
    props: values => ({
      disabled: [200, 201, 202, 204, 205, 300, 301, 302, 304, 305].includes(values?.status),
    }),
  })
  @I18n('meta.Sources.HaBinlog.FileName')
  'startDumpPosition.entryPosition.journalName': string;

  @FieldDecorator({
    type: 'inputnumber',
    props: values => ({
      disabled: [200, 201, 202, 204, 205, 300, 301, 302, 304, 305].includes(values?.status),
      min: 1,
      max: 1000000000,
      precision: 0,
    }),
  })
  @I18n('meta.Sources.HaBinlog.FileLocation')
  'startDumpPosition.entryPosition.position': number;
}
