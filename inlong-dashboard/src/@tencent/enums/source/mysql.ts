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

export enum ReadTypeEnum {
  FULL = '0',
  INC = '1',
}

export enum AllMigrationEnum {
  YES = 1,
  NO = 0,
}

export const readTypeMap: Map<ReadTypeEnum, string> = (() => {
  return new Map([
    [ReadTypeEnum.FULL, '全量'],
    [ReadTypeEnum.INC, '增量'],
  ]);
})();

export const allMigrationMap: Map<AllMigrationEnum, string> = (() => {
  return new Map([
    [AllMigrationEnum.YES, '是'],
    [AllMigrationEnum.NO, '否'],
  ]);
})();