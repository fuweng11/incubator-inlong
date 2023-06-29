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
import { Table, Button, Icon } from '@tencent/tea-component';
import { ProForm, ProFormProps } from '@tencent/tea-material-pro-table';
import React, { useState } from 'react';
import PageStatus from '@/@tencent/components/PageStatus';
import { TestEmpty } from '@/@tencent/components/Icons';
import request from '@/core/utils/request';
import moment from 'moment';
import isEmpty from 'lodash/isEmpty';

const { scrollable } = Table.addons;
const infoStyle: React.CSSProperties = { marginTop: -3, marginRight: 2 };

const Test = ({ info }) => {
  const [testRes, setTestRes] = useState<any>([]);
  const [loading, setLoading] = useState<any>(false);
  const handleTest = async values => {
    setLoading(true);
    const data = await request({
      url: '/data/preview/query',
      method: 'POST',
      data: {
        count: values.count,
        inLongGroupID: info.inLongGroupID,
        inLongStreamID: info.inLongStreamID,
      },
    });
    setLoading(false);
    setTestRes(data);
  };

  const infoTips = (text: String) => (
    <span>
      <Icon type="info" style={infoStyle} />
      <span style={{ color: '#bbb' }}>{text}</span>
    </span>
  );

  const fields: ProFormProps['fields'] = [
    {
      name: 'count',
      type: 'number',
      title: '抽样数量',
      required: true,
      component: 'inputNumber',
      defaultValue: 10,
      min: 1,
      max: 50,
      suffix: infoTips('请配置小于等于50条的抽样数量'),
    },
  ];

  return (
    <>
      <ProForm
        style={{ marginTop: '20px' }}
        fields={fields}
        submitter={{
          render: form => (
            <div>
              <Button type="primary" loading={loading} onClick={() => form.submit()}>
                开始抽样
              </Button>
              {infoTips('仅抽取最新上报的数据')}
            </div>
          ),
        }}
        onFinish={handleTest}
      />
      <div style={{ marginTop: '10px' }}>
        {!isEmpty(testRes) ? (
          <>
            <Table
              bordered
              verticalTop
              records={testRes}
              recordKey="id"
              columns={[
                {
                  key: 'dt',
                  header: '上报时间戳',
                  render: (row: any) => moment.unix(row.dt / 1000).format('YYYY-MM-DD HH:mm'),
                },
                {
                  key: 'body',
                  header: '原始数据',
                },
                {
                  key: 'nodeIp',
                  header: '上报节点',
                },
              ]}
              addons={[
                // 支持表格滚动，高度超过 192 开始显示滚动条
                scrollable({
                  maxHeight: 392,
                  // minWidth: 3600,
                  onScrollBottom: () => console.log('到达底部'),
                }),
              ]}
            />
          </>
        ) : (
          <PageStatus icon={<TestEmpty />} description="暂无数据" />
        )}
      </div>
    </>
  );
};

export default Test;
