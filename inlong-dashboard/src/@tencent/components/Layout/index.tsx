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

import React, { useCallback, useMemo } from 'react';
import { config } from '@/configs/default';
import { Layout, Menu, NavMenu } from 'tea-component';
import menuTree, { MenuItemType } from '@/configs/menus';
import { useSelector } from '@/hooks';
import { Link } from 'react-router-dom';
import { State } from '@/models';

import 'tea-component/dist/tea.css';

const { Header, Body, Sider, Content } = Layout;

const BasicLayout: React.FC = props => {
  const currentMenu = useSelector<State, State['currentMenu']>(state => state.currentMenu);
  const userName = useSelector<State, State['userName']>(state => state.userName);

  const subMenuTree = useMemo(() => {
    const TopKey = currentMenu?.deepKey?.[0];
    return menuTree.find(item => item.key === TopKey);
  }, [currentMenu]);

  const getFirstChildRoute = useCallback((menuItem: MenuItemType) => {
    if (!menuItem.children && menuItem.path) return menuItem.path;
    if (menuItem.children) return getFirstChildRoute(menuItem.children[0]);
  }, []);

  return (
    <Layout>
      <Header>
        <NavMenu
          left={
            <>
              <NavMenu.Item type="logo">
                <img src={config.logo} alt="logo" style={{ maxHeight: '100%', width: 120 }} />
              </NavMenu.Item>

              {menuTree.map(item => (
                <NavMenu.Item key={item.key} selected={item.key === currentMenu?.deepKey?.[0]}>
                  <Link to={getFirstChildRoute(item)}>{item.name}</Link>
                </NavMenu.Item>
              ))}
            </>
          }
          right={
            <>
              <NavMenu.Item>{userName}</NavMenu.Item>
            </>
          }
        />
      </Header>
      <Body>
        <Sider>
          <Menu collapsable theme="dark" title="产品名称">
            {subMenuTree?.children?.map(item => (
              <Menu.Item
                key={item.key}
                title={item.name}
                selected={item.key === currentMenu?.deepKey?.[1]}
                render={children => <Link to={item.path}>{children}</Link>}
              />
            ))}
          </Menu>
        </Sider>

        <Content>{props.children}</Content>
      </Body>
    </Layout>
  );
};

export default BasicLayout;
