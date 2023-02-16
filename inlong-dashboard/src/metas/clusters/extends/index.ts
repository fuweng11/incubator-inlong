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

import type { MetaExportWithBackendList } from '@/metas/types';
import type { ClusterMetaType } from '../types';

export const allExtendsClusters: MetaExportWithBackendList<ClusterMetaType> = [
  // You can extends at here...
  {
    label: 'ZooKeeper',
    value: 'ZOOKEEPER',
    LoadEntity: () => import('./ZooKeeper'),
  },
  {
    label: 'DbsyncZK',
    value: 'DBSYNC_ZK',
    LoadEntity: () => import('./Dbsync'),
  },
  {
    label: 'SortCKTask',
    value: 'SORT_CK',
    LoadEntity: () => import('./sort/SortCKTask'),
  },
  {
    label: 'SortESTask',
    value: 'SORT_ES',
    LoadEntity: () => import('./sort/SortESTask'),
  },
  {
    label: 'SortHiveTask',
    value: 'SORT_HIVE',
    LoadEntity: () => import('./sort/SortHiveTask'),
  },
  {
    label: 'SortTHiveTask',
    value: 'SORT_THIVE',
    LoadEntity: () => import('./sort/SortTHiveTask'),
  },
  {
    label: 'SortIcebergTask',
    value: 'SORT_ICEBERG',
    LoadEntity: () => import('./sort/SortIcebergTask'),
  },
  {
    label: 'SortStarRocksTask',
    value: 'SORT_STARROCKS',
    LoadEntity: () => import('./sort/SortStarRocksTask'),
  },
];
