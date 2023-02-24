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
import ContentRadio from '@/@tencent/components/ContentRadio';
import FileUpload from '@/@tencent/components/FileUpload';
import { TabPanel, Tabs, Status } from '@tencent/tea-component';
import { ProForm, ProFormProps } from '@tencent/tea-material-pro-table';
import React, { useRef, useState } from 'react';
import Editor from '@monaco-editor/react';

const tabs = [
  { id: 'result', label: '调试结果' },
  { id: 'log', label: '调试日志' },
];

const STATUSMAP = {
  success: {
    text: '运行成功',
    color: '#01CBAB',
  },
  failed: {
    text: '运行失败',
    color: '#E54545',
  },
};

const Test = () => {
  const [testRes, setTestRes] = useState<any>('');
  const onlineRef = useRef<any>('');
  const fileRef = useRef<any>('');
  const handleTest = async values => {
    const mockRes = {
      status: 'success',
      time: '2022-01-11 10:23:23',
      result: '这是调试结果',
      log: '这是调试日志',
    };
    const res = await new Promise(resolve => {
      setTimeout(() => {
        resolve(mockRes);
      }, 1000);
    });
    setTestRes(res);
  };

  const fields: ProFormProps['fields'] = [
    {
      name: 'source',
      type: 'string',
      title: '数据来源',
      required: true,
      component: ContentRadio,
      reaction: (field, values) => {
        //设置默认值
        field.setValue('online');
        //设置组件参数
        field.setComponentProps({
          style: { width: '80%' },
          radios: [
            {
              value: 'online',
              text: '在线抽样',
              operations: (
                <ProForm
                  style={{ padding: '0', marginBottom: '-10px' }}
                  fields={[
                    {
                      name: 'number',
                      type: 'number',
                      title: '抽样数量',
                      suffix: (
                        <span style={{ color: '#bbb' }}>（请配置小于等于1,000条的抽样数量）</span>
                      ),
                      required: true,
                      component: 'inputNumber',
                      defaultValue: 100,
                      max: 1000,
                      min: 1,
                      unit: '条',
                    },
                  ]}
                  onRef={form => (onlineRef.current = form)}
                  submitter={false}
                />
              ),
            },
            {
              value: 'file',
              text: '文件上传',
              operations: (
                <ProForm
                  style={{ padding: '0', marginBottom: '-10px' }}
                  fields={[
                    {
                      name: 'file',
                      type: 'string',
                      required: true,
                      component: () => (
                        <FileUpload
                          message="请上传CSV、KV文件，大小在10M以内"
                          action={''} //TODO上传地址
                          maxSize={10 * 1024 * 1024}
                          accept={['.csv', '.kv']}
                        />
                      ),
                    },
                  ]}
                  onRef={form => (fileRef.current = form)}
                  submitter={false}
                />
              ),
            },
          ],
        });
      },
    },
    {
      name: 'subscribe',
      type: 'boolean',
      title: '数据订阅',
      defaultValue: false,
      component: 'switch',
      reaction: (fields, values) => {
        fields.setComponentProps({
          children: !values.subscribe ? '暂不开启' : '开启',
        });
      },
    },
  ];

  return (
    <>
      <ProForm
        style={{ marginTop: '20px' }}
        fields={fields}
        submitter={{
          submitText: '开始测试',
          resetText: '重置参数',
        }}
        onFinish={handleTest}
      />
      <div style={{ marginTop: '10px' }}>
        {testRes ? (
          <>
            <div style={{ border: '1px solid #DDD' }}>
              <Tabs
                tabs={tabs}
                tabBarStyle={{
                  padding: '5px 10px',
                }}
                addon={
                  <div style={{ lineHeight: '30px', verticalAlign: 'middle' }}>
                    <span style={{ color: '#444', display: 'inline-block', marginRight: '20px' }}>
                      {testRes.time}测试结果
                    </span>
                    <span>
                      调试状态：
                      <span style={{ color: STATUSMAP[testRes.status].color }}>
                        {STATUSMAP[testRes.status].text}
                      </span>
                    </span>
                  </div>
                }
              >
                {tabs.map(tab => (
                  <TabPanel id={tab.id} key={tab.id}>
                    <Editor height="150px" value={testRes[tab.id]} options={{ readOnly: true }} />
                  </TabPanel>
                ))}
              </Tabs>
            </div>
          </>
        ) : (
          <Status icon="blank" title="暂无数据" size="s" />
        )}
      </div>
    </>
  );
};

export default Test;
