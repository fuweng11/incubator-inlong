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
import { MenuItemType } from '@/configs/menus';
import React from 'react';
import { Link } from 'react-router-dom';
import { Layout, Menu } from '@tencent/tea-component';
import styles from './index.module.less';

const { Body, Sider, Content } = Layout;

export interface LayoutBodyProps {
  subMenuTree: MenuItemType[];
  currentMenu: MenuItemType;
}

const LayoutBody: React.FC<LayoutBodyProps> = ({ subMenuTree, currentMenu, children }) => {
  return (
    <Body>
      <Sider style={{ zIndex: 100 }}>
        <Menu collapsable theme="dark">
          {subMenuTree &&
            subMenuTree.map((menu, i) => {
              return (
                <Menu.Item
                  className={styles.subMenuItem}
                  selected={menu.key == currentMenu.key}
                  key={menu.key}
                  title={menu.name}
                  render={children => <Link to={menu.path as string}>{children}</Link>}
                ></Menu.Item>
              );
            })}
        </Menu>
      </Sider>
      <Content>{children}</Content>
    </Body>
  );
};

export default LayoutBody;
