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

import { useCallback, useEffect } from 'react';
import { useSelector } from 'react-redux';
import { State } from '@/core/stores';

const ids = {
  dev: 'zhTEyhFupNbHirMBni',
  test: 'zhTEyhFuQgKBqCIMzX',
  prod: 'zhTEyhFumjJJdzHfCs',
};

const currentId = (() => {
  const { host } = window.location;
  let tamEnv = '';
  if (host === 'wedata-dev.woa.com') {
    tamEnv = 'prod';
  } else if (host.indexOf('-test') !== -1) {
    tamEnv = 'test';
  } else if (host.indexOf('-dev') !== -1) {
    tamEnv = 'dev';
  }
  console.log('use tam env: ', tamEnv || null);
  return ids[tamEnv];
})();

export const useTam = () => {
  const userName = useSelector<State, State['userName']>(state => state.userName);

  const create = useCallback((uin = '') => {
    const script = document.createElement('script');
    script.src = 'https://tam.cdn-go.cn/aegis-sdk/latest/aegis.min.js';
    script.onload = () => {
      // @ts-ignore
      new Aegis({
        id: currentId, // 项目ID，即上报id
        uin, // 用户唯一 ID（可选）
        reportApiSpeed: true, // 接口测速
        reportAssetSpeed: true, // 静态资源测速
        spa: true, // spa 页面开启
      });
    };
    document.head.appendChild(script);
  }, []);

  useEffect(() => {
    if (userName) {
      create(userName);
    }
  }, [create, userName]);
};
