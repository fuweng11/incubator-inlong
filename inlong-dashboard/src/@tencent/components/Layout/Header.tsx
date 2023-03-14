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
import React, { useCallback } from 'react';
import { Dropdown, Layout, List, NavMenu } from '@tencent/tea-component';
import styles from './index.module.less';
import menuTree, { MenuItemType } from '@/configs/menus';
import { Link } from 'react-router-dom';
import HeaderDropdown from '@tencent/tea-dcpg/dist/Header';
import { config } from '@/configs/default';

const { Header } = Layout;

export interface LayoutHeaderProps {
  currentMenu: MenuItemType;
  userName: string;
}

const LayoutHeader: React.FC<LayoutHeaderProps> = ({
  currentMenu,
  userName,
}: LayoutHeaderProps) => {
  const getFirstChildRoute = useCallback((menuItem: MenuItemType) => {
    if (!menuItem.children && menuItem.path) return menuItem.path;
    if (menuItem.children) return getFirstChildRoute(menuItem.children[0]);
  }, []);

  const projectList = () => (
    <List className={styles.projectList}>
      <List.Item className={styles.projectListItem}>项目1</List.Item>
    </List>
  );
  return (
    <Header>
      <NavMenu
        left={
          <>
            <NavMenu.Item>
              <HeaderDropdown />
            </NavMenu.Item>
            <NavMenu.Item type="logo" onClick={() => window.open('/', '_self')}>
              <img src={config.logo} alt="logo" />
            </NavMenu.Item>
            <div style={{ marginLeft: '5px', color: 'rgba(255, 255, 255, 0.45)', opacity: '0.6' }}>
              |
            </div>
            <span style={{ color: '#FFFFFF', opacity: 0.8, margin: ' 0 15px', fontSize: '14px' }}>
              数据集成
            </span>
            <Dropdown
              className={styles.dropdown}
              children={projectList}
              trigger="hover"
              boxClassName={styles.dropdownBox}
              button={<div className={styles.dropdownButton}>当前项目</div>}
            ></Dropdown>
            <div style={{ marginLeft: '15px', color: 'rgba(255, 255, 255, 0.45)', opacity: '0.6' }}>
              |
            </div>
            {menuTree.map((menu, i) => {
              return (
                currentMenu && (
                  <NavMenu.Item className={styles.headerMenuItem} key={i}>
                    <Link
                      to={getFirstChildRoute(menu)}
                      className={
                        menu.key == currentMenu.key || menu.key == currentMenu.deepKey[0]
                          ? styles.activeText
                          : styles.noActiveText
                      }
                    >
                      {menu.name}
                    </Link>
                  </NavMenu.Item>
                )
              );
            })}
          </>
        }
        right={
          <div className={styles.userInfo}>
            <img
              className={styles.avatar}
              src={
                userName
                  ? `//dayu.woa.com/avatars/${userName}/profile.jpg`
                  : '//dayu.woa.com/avatars/profile.gif'
              }
              alt="avatar"
            />
            <span>{userName}</span>
          </div>
        }
      />
    </Header>
  );
};

export default LayoutHeader;
