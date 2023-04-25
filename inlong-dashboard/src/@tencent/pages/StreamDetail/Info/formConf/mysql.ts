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

import { allSyncMap, snapshotModeMap } from '@/@tencent/enums/source/mysql';
import type { FormConfItem } from '.';

export const fields: FormConfItem['fields'] = [
  { label: '数据库', value: 'dataBaseName' },
  { label: '是否整库同步', value: 'allSync', enumMap: allSyncMap },
  { label: '表名白名单', value: 'tableWhiteList' },
  { label: '读取方式', value: 'snapshotMode', enumMap: snapshotModeMap },
];
