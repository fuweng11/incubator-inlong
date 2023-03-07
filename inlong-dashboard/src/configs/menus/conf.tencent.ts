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

import { StreamManage, SubscribeManage, AcquisitionManage } from '@/@tencent/components/Icons';
import type { MenuItemType } from '.';

const conf: MenuItemType[] = [
  {
    path: '/demo',
    name: 'Example',
  },
  {
    name: '数据接入',
    children: [
      {
        path: '/stream',
        name: '接入管理',
        icon: StreamManage({}),
      },
      {
        path: '/subscribe',
        name: '数据订阅',
        icon: SubscribeManage({}),
      },
      {
        path: '/collector',
        name: '采集器管理',
        icon: AcquisitionManage({}),
      },
    ],
  },
];

export default conf;
