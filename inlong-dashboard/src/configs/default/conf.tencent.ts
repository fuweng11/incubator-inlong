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

import Provider from '@/@tencent/components/Provider';
import Layout from '@/@tencent/components/Layout';
import PageLoading from '@/@tencent/components/PageLoading';
import { requestErrorAlert } from '@/@tencent/components/RequestErrorAlert';

const conf = {
  title: '',
  logo: '//imgcache.qq.com/qcloud/tcloud_dtc/static/We_Data/cdccaf86-75d2-4c94-9ebd-4661a8271c95.svg',
  useLogin: false,
  redirectRoutes: {
    '/': '/stream',
  } as Record<string, string>,
  loginUrl: '/',
  AppProvider: Provider,
  AppLoading: PageLoading,
  AppLayout: Layout,
  requestPrefix: '/api/wedata/inlong/service',
  requestErrorAlert,
};

export default conf;
