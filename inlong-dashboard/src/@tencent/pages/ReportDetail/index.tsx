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

import React, { useState } from 'react';
import { useParams } from 'react-router-dom';
import { Alert, Button, Tag, Tabs, TabPanel } from '@tencent/tea-component';
import { useRequest } from 'ahooks';
import { PageContainer, Container } from '@/@tencent/components/PageContainer';
import Description from '@/@tencent/components/Description';
import PublishModal from '@/@tencent/pages/Stream/PublishModal';
import { dataLevelMap } from '@/@tencent/enums/stream';
import Info from './Info';
import SubscribeList from './Subscribe';
import Test from './Test';

const tabs = [
  { id: 'info', label: '基本信息', Component: Info },
  { id: 'test', label: '数据测试', Component: Test },
  { id: 'subscribe', label: '数据订阅', Component: SubscribeList },
  { id: 'statistic', label: '数据统计', Component: () => <div />, disabled: true },
  { id: 'schema', label: 'schema管理', Component: () => <div />, disabled: true },
  { id: 'devops', label: '采集治理', Component: () => <div />, disabled: true },
];

export default function StreamDetail() {
  const { id: streamId } = useParams<{ id: string }>();

  const [publishModal, setPublishModal] = useState<{ visible: boolean; id?: string }>({
    visible: false,
  });

  const { data = {} } = useRequest({
    url: '/access/query/info',
    method: 'POST',
    data: {
      projectID: '1',
      streamID: streamId,
    },
  });

  const { data: subscribeData = {} } = useRequest({
    url: '/subscribe/all/list',
    method: 'POST',
    data: {
      projectID: '1',
      streamID: streamId,
      pagesize: 1,
      currPage: 1,
    },
  });

  return (
    <PageContainer useDefaultContainer={false} breadcrumb={[{ name: '接入详情' }]}>
      {!subscribeData?.total && (
        <Alert type="info">
          当前日志尚未配置数据订阅，如有数据消费需求，请进入【数据订阅】配置。
          <Button type="link" style={{ textDecoration: 'underline' }}>
            立即配置
          </Button>
        </Alert>
      )}

      <Container>
        <Description
          column={4}
          title={
            <>
              <Tag theme="warning" style={{ marginTop: 0 }}>
                {data.status}
              </Tag>
              <span>{`${data.name}（ID:${data.streamID}）`}</span>
            </>
          }
          extra={
            <Button type="link" onClick={() => setPublishModal({ visible: true, id: streamId })}>
              发布上线
            </Button>
          }
        >
          <Description.Item title="创建人">创建人</Description.Item>
          <Description.Item title="创建时间">{data.creatTime}</Description.Item>
          <Description.Item title="数据分级">{dataLevelMap.get(data.dataLevel)}</Description.Item>
          <Description.Item title="描述" span={3}>
            {data.remark}
          </Description.Item>
        </Description>
      </Container>

      <Container>
        <Tabs tabs={tabs}>
          {tabs.map(({ id, Component }) => (
            <TabPanel id={id} key={id}>
              <Component streamId={streamId} />
            </TabPanel>
          ))}
        </Tabs>
      </Container>

      <PublishModal
        {...publishModal}
        onOk={data => setPublishModal({ visible: false })}
        onClose={() => setPublishModal({ visible: false })}
      />
    </PageContainer>
  );
}
