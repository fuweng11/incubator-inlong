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

import React from 'react';
import { useSelector } from 'react-redux';
import { useHistory } from 'react-router-dom';
import { Layout, Breadcrumb } from '@tencent/tea-component';
import { State } from '@/core/stores';
import Container from './Container';
import styles from './Page.module.less';

const { Content } = Layout;

export type BreadcrumbItemType = {
  name: string;
  path?: string;
};

export interface PageContainerProps {
  className?: string;
  // style
  style?: React.CSSProperties;
  breadcrumb?: BreadcrumbItemType[];
  // Whether to automatically generate breadcrumbs for the current menu
  useDefaultBreadcrumb?: boolean;
  // Whether to automatically use Container to wrap the content area
  useDefaultContainer?: boolean;
  children?: React.ReactNode;
}

const Page: React.FC<PageContainerProps> = ({
  className = '',
  style,
  children,
  useDefaultBreadcrumb = true,
  useDefaultContainer = true,
  breadcrumb = [],
}) => {
  const history = useHistory();
  const currentMenu = useSelector<State, State['currentMenu']>(state => state.currentMenu);

  // const defaultBreadcrumb = [{ name: 'Home', path: '/' }] as BreadcrumbItem[];
  const defaultBreadcrumb = [];
  if (currentMenu) {
    const { name, path } = currentMenu;
    if (name && path) defaultBreadcrumb.push({ name, path });
  }

  const breadcrumbs = useDefaultBreadcrumb ? defaultBreadcrumb.concat(breadcrumb) : breadcrumb;

  return (
    <Content className={`${styles.panel} ${className}`} style={style}>
      <Content.Header
        showBackButton={breadcrumbs.length > 1}
        onBackButtonClick={() => history.goBack()}
        title={
          <>
            <Breadcrumb>
              {breadcrumbs?.map(item => (
                <Breadcrumb.Item key={item.path || item.name}>{item.name}</Breadcrumb.Item>
              ))}
            </Breadcrumb>
          </>
        }
      />

      <Content.Body>
        {useDefaultContainer ? <Container>{children}</Container> : children}
      </Content.Body>
    </Content>
  );
};

export default React.memo(Page);
