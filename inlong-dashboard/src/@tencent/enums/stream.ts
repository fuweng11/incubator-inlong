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

export enum DataLevelEnum {
  L1 = 1,
  L2 = 2,
  L3 = 3,
  L4 = 4,
  L5 = 5,
}

export const dataLevelMap: Map<DataLevelEnum, string> = (() => {
  return new Map([
    [DataLevelEnum.L1, '一级'],
    [DataLevelEnum.L2, '二级'],
    [DataLevelEnum.L3, '三级'],
    [DataLevelEnum.L4, '四级'],
    [DataLevelEnum.L5, '五级'],
  ]);
})();

export enum StatusEnum {
  New = 100,
  Configuring = 110,
  Success = 130,
  Failed = 120,
  Deleted = 40,
  Deleting = 41,
  Abnormal = 500,
  Approval = 101,
  Rejected = 102,
  Passed = 103,
  Canceled = 104,
}

export const statusMap: Map<
  StatusEnum,
  { label: string; colorTheme: 'success' | 'warning' | 'default' | 'error' }
> = (() => {
  return new Map([
    [
      StatusEnum.Canceled,
      {
        label: '已取消',
        colorTheme: 'warning',
      },
    ],
    [
      StatusEnum.Passed,
      {
        label: '审批通过',
        colorTheme: 'success',
      },
    ],
    [
      StatusEnum.Rejected,
      {
        label: '已拒绝',
        colorTheme: 'error',
      },
    ],
    [
      StatusEnum.Approval,
      {
        label: '待审批',
        colorTheme: 'warning',
      },
    ],
    [
      StatusEnum.New,
      {
        label: '待上线',
        colorTheme: 'warning',
      },
    ],
    [
      StatusEnum.Success,
      {
        label: '已上线',
        colorTheme: 'success',
      },
    ],
    [
      StatusEnum.Configuring,
      {
        label: '配置中',
        colorTheme: 'warning',
      },
    ],
    [
      StatusEnum.Failed,
      {
        label: '配置失败',
        colorTheme: 'warning',
      },
    ],
    [
      StatusEnum.Deleted,
      {
        label: '下线',
        colorTheme: 'default',
      },
    ],
    [
      StatusEnum.Deleting,
      {
        label: '下线中',
        colorTheme: 'default',
      },
    ],
    [
      StatusEnum.Abnormal,
      {
        label: '异常',
        colorTheme: 'error',
      },
    ],
  ]);
})();

export enum AccessTypeEnum {
  SDK = 'AUTO_PUSH',
  FILE = 'FILE',
  DB = 'DB',
}

export const accessTypeMap: Map<AccessTypeEnum, string> = (() => {
  return new Map([
    [AccessTypeEnum.SDK, 'SDK'],
    [AccessTypeEnum.FILE, '文件'],
    [AccessTypeEnum.DB, 'DB'],
  ]);
})();

export enum EncodeTypeEnum {
  UTF8 = 'UTF-8',
  GBK = 'GBK',
}

export const encodeTypeMap: Map<EncodeTypeEnum, string> = (() => {
  return new Map([
    [EncodeTypeEnum.UTF8, 'UTF-8'],
    [EncodeTypeEnum.GBK, 'GBK'],
  ]);
})();

export enum PeakRateEnum {
  L1 = 100,
  L2 = 1000,
  L3 = 5000,
  L4 = 10000,
  L5 = 50000,
  L6 = 100000,
  L7 = 500000,
  L8 = 1000000,
}

export const peakRateMap: Map<PeakRateEnum, string> = (() => {
  return new Map([
    [PeakRateEnum.L1, '100及以下'],
    [PeakRateEnum.L2, '100到1000之间'],
    [PeakRateEnum.L3, '1000到5000之间'],
    [PeakRateEnum.L4, '5000到1W之间'],
    [PeakRateEnum.L5, '1W到5W之间'],
    [PeakRateEnum.L6, '5W到10W之间'],
    [PeakRateEnum.L7, '10W到50W之间'],
    [PeakRateEnum.L8, '50W到100W之间'],
  ]);
})();

export enum DataSeparatorEnum {
  Space = '32',
  VerticalLine = '124',
  Comma = '44',
  Semicolon = '59',
  Asterisk = '42',
  DoubleQuotes = '34',
}

export const dataSeparatorMap: Map<DataSeparatorEnum, string> = (() => {
  return new Map([
    [DataSeparatorEnum.Space, '空格( )'],
    [DataSeparatorEnum.VerticalLine, '竖线(|)'],
    [DataSeparatorEnum.Comma, '逗号(,)'],
    [DataSeparatorEnum.Semicolon, '分号(;)'],
    [DataSeparatorEnum.Asterisk, '星号(*)'],
    [DataSeparatorEnum.DoubleQuotes, '双引号(“)'],
  ]);
})();
