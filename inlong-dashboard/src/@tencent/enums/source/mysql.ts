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

export enum SnapshotModeEnum {
  FULL = '0',
  INC = '1',
}

export enum AllSyncEnum {
  YES = 1,
  NO = 0,
}

export const snapshotModeMap: Map<SnapshotModeEnum, string> = (() => {
  return new Map([
    [SnapshotModeEnum.FULL, '全量'],
    [SnapshotModeEnum.INC, '增量'],
  ]);
})();

export const allSyncMap: Map<AllSyncEnum, string> = (() => {
  return new Map([
    [AllSyncEnum.YES, '是'],
    [AllSyncEnum.NO, '否'],
  ]);
})();
