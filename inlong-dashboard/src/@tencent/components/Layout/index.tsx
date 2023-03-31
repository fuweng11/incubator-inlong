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

import React, { useMemo } from 'react';
import { Layout } from '@tencent/tea-component';
import menuTree from '@/configs/menus';
import { useSelector } from 'react-redux';
import { State } from '@/core/stores';
import Header from './Header';
import Body from './Body';

const BasicLayout: React.FC = props => {
  const currentMenu = useSelector<State, State['currentMenu']>(state => state.currentMenu);
  const userName = useSelector<State, State['userName']>(state => state.userName);

  const subMenuTree = useMemo(() => {
    const TopKey = currentMenu?.deepKey?.[0];
    return menuTree.find(item => item.key === TopKey)?.children;
  }, [currentMenu]);

  return (
    <Layout style={{ minWidth: 1000 }}>
      <Header currentMenu={currentMenu} userName={userName} />
      <Body children={props.children} subMenuTree={subMenuTree} currentMenu={currentMenu} />
    </Layout>
  );
};

export default BasicLayout;
