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

import { useRequest } from 'ahooks';
import { BaseOptions } from '@ahooksjs/use-request/lib/types';

const PlatformPrefix = '/api/wedata/tdw/platform';

const PlatformResponseParse = result => ({
  success: result.code === 'SUCCESS',
  data: result.data?.Data,
  errMsg: result.message,
});

export const useCurrentUser = (options?: BaseOptions<any, any>) =>
  useRequest(
    {
      url: '/DescribeCurrentUserInfo',
      method: 'POST',
      prefix: PlatformPrefix,
      responseParse: PlatformResponseParse,
    },
    options,
  );

export const useProjectComputeResources = (projectId, options?: BaseOptions<any, any>) =>
  useRequest(
    {
      url: '/DescribeProjectResourceGroupComputeResources',
      method: 'POST',
      prefix: PlatformPrefix,
      data: {
        Action: 'DescribeProjectResourceGroupComputeResources',
        Params: {
          DescribeAuthData: true,
          DescribeCreator: true,
          DescribeRealTimeMonitorData: true,
          ProjectId: projectId,
        },
      },
      responseParse: PlatformResponseParse,
    },
    options,
  );

export const useProjectList = (options?: BaseOptions<any, any>) =>
  useRequest(
    {
      url: '/DescribeUserProjects',
      method: 'POST',
      prefix: PlatformPrefix,
      data: {
        Action: 'DescribeUserProjects',
        Params: {
          DescribeCreator: true,
        },
      },
      responseParse: PlatformResponseParse,
    },
    options,
  );

export const useProjectInfo = (projectId, options?: BaseOptions<any, any>) =>
  useRequest(
    {
      url: '/DescribeProject',
      method: 'POST',
      prefix: PlatformPrefix,
      data: {
        Action: 'DescribeProject',
        Params: {
          DescribeCreator: true,
          ProjectId: projectId,
        },
      },
      responseParse: PlatformResponseParse,
    },
    options,
  );
