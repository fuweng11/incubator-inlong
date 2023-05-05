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

import { encodeTypeMap, dataSeparatorMap, peakRateMap } from '@/@tencent/enums/stream';
import { SourceTypeEnum, sourceTypeMap } from '@/@tencent/enums/source';
import type { FieldItemType } from '@/@tencent/enums/source/common';
import { fields as fileFields } from '@/@tencent/enums/source/file';
import { fields as mysqlFields } from '@/@tencent/enums/source/mysql';
import { fields as postgreSqlFields } from '@/@tencent/enums/source/postgreSql';

interface FormConfItem {
  title?: string;
  fields: FieldItemType[];
}

export const getFields = (accessModel): FormConfItem[] =>
  [
    {
      fields: [
        { label: '接入方式', value: 'accessModel', enumMap: sourceTypeMap },
        { label: 'group id', value: 'inLongGroupID' },
        { label: 'stream id', value: 'inLongStreamID' },
      ],
    },
    {
      title: '数据流量',
      fields: [
        { label: '单日峰值', value: 'peakRate', enumMap: peakRateMap },
        { label: '单日最大接入量', value: 'peakTotalSize', unit: 'GB' },
        { label: '单条数据最大值', value: 'msgMaxLength', unit: 'Byte' },
      ],
    },
    accessModel &&
      accessModel !== SourceTypeEnum.SDK && {
        title: '数据源信息',
        fields: {
          [SourceTypeEnum.FILE]: fileFields,
          [SourceTypeEnum.MySQL]: mysqlFields,
          [SourceTypeEnum.PostgreSQL]: postgreSqlFields,
        }[accessModel],
      },
    {
      title: '数据格式',
      fields: [
        { label: '编码类型', value: 'encodeType', enumMap: encodeTypeMap },
        { label: '分隔符', value: 'dataSeparator', enumMap: dataSeparatorMap },
      ],
    },
  ].filter(Boolean);
