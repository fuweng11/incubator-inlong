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
import { Alert, Button, Tag, Tabs, TabPanel } from '@tencent/tea-component';
import { PageContainer, Container } from '@/@tencent/components/PageContainer';
import Description from '@/@tencent/components/Description';
import Info from './Info';
import SubscribeList from './Subscribe';

export default function TabsExample() {
  const tabs = [
    { id: 'info', label: '基本信息', Component: Info },
    { id: 'test', label: '数据测试', Component: Info },
    { id: 'subscribe', label: '数据订阅', Component: SubscribeList },
    { id: 'statistic', label: '数据统计', Component: Info, disabled: true },
    { id: 'schema', label: 'schema管理', Component: Info, disabled: true },
    { id: 'devops', label: '采集治理', Component: Info, disabled: true },
  ];

  return (
    <PageContainer useDefaultContainer={false} breadcrumb={[{ name: '接入详情' }]}>
      <Alert type="info">
        当前日志尚未配置数据订阅，如有数据消费需求，请进入【数据订阅】配置。
        <Button type="link" style={{ textDecoration: 'underline' }}>
          立即配置
        </Button>
      </Alert>

      <Container>
        <Description
          column={5}
          title={
            <>
              <Tag theme="warning" style={{ marginTop: 0 }}>
                这是一个标签
              </Tag>
              <span>数据流AAAA我是日志名称（ID:907181027）</span>
            </>
          }
          extra={<Button type="primary">发布上线</Button>}
        >
          <Description.Item title="创建人">admin</Description.Item>
          <Description.Item title="创建时间">2022-01-01</Description.Item>
          <Description.Item title="aa">aa</Description.Item>
          <Description.Item title="bb">bb</Description.Item>
          <Description.Item title="cc">cc</Description.Item>
        </Description>
      </Container>

      <Container>
        <Tabs tabs={tabs}>
          {tabs.map(({ id, Component }) => (
            <TabPanel id={id} key={id}>
              <Component />
            </TabPanel>
          ))}
        </Tabs>
      </Container>
    </PageContainer>
  );
}
