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
import FieldsMap from '@/@tencent/components/FieldsMap';
import { FieldData } from '@/@tencent/components/FieldsMap';
import { Drawer, Button, Row, Col, Form } from '@tencent/tea-component';
import { ProForm, ProFormProps } from '@tencent/tea-material-pro-form';
import React, { useRef, useState } from 'react';

export interface AddSubscribeDrawerProps {
  visible: boolean;
  onClose: () => void;
}

const AddSubscribeDrawer = ({ visible, onClose }: AddSubscribeDrawerProps) => {
  const defaultRecords = [
    {
      id: 0,
      fieldName: '测试longtext：test2352vrgrlmgklrtngktrgnkrtnrtjngrtnnbktrjnvtr',
      fieldType: 'string',
      fieldComment: '备注备至备注备至备注备至备注备至备注备至备注备至',
    },
    {
      id: 1,
      fieldName: 'test1',
      fieldType: 'string',
      fieldComment: '备注1',
    },
    {
      id: 2,
      fieldName: 'test2',
      fieldType: 'string',
      fieldComment: '备注1',
    },
    {
      id: 3,
      fieldName: 'test3',
      fieldType: 'string',
      fieldComment: '备注1',
    },
    {
      id: 4,
      fieldName: 'test4',
      fieldType: 'string',
      fieldComment: '备注1',
    },
  ];
  const [targetFields, setTargetFields] = useState<FieldData>(defaultRecords);
  const [selectedFields, setSelectedFields] = useState<any>([]);
  const [isFieldsMapVisible, setIsFieldsMapVisible] = useState<boolean>(false);
  const formRef = useRef<any>();
  const autoCreateTableRef = useRef<any>();
  const selectTableRef = useRef<any>();

  const formFields: ProFormProps['fields'] = [
    {
      name: 'writeType',
      type: 'string',
      title: '写入类型',
      required: true,
      component: 'select',
      appearance: 'button',
      style: { width: '80%' },
      defaultValue: 'HIVE',
      disabled: true,
      options: [
        {
          value: 'HIVE',
          text: 'HIVE',
        },
      ],
    },
    {
      name: 'sperate',
      type: 'string',
      title: '分区间隔',
      required: true,
      component: 'radio',
      defaultValue: 'hour',
      options: [
        {
          name: 'hour',
          title: '小时',
        },
        {
          name: 'day',
          title: '天',
        },
      ],
      reaction: (fields, values) => {
        fields.setComponentProps({
          suffix: (
            <span style={{ color: '#888', display: 'inline-block', marginLeft: '10px' }}>
              分区字段：{values.sperate}
            </span>
          ),
        });
      },
    },
    {
      name: 'database',
      type: 'string',
      title: '数据库名称',
      required: true,
      component: 'select',
      appearance: 'button',
      style: { width: '80%' },
      defaultValue: 'HIVE',
      options: [
        {
          value: 'HIVE',
          text: 'HIVE',
        },
      ],
      suffix: (
        <a href="/" target="_blank">
          申请资源
        </a>
      ),
    },
    {
      name: 'tableType',
      type: 'string',
      title: '数据来源',
      required: true,
      component: ContentRadio,
      reaction: (field, values) => {
        field.setComponentProps({
          style: { width: '80%' },
          radios: [
            {
              value: 'auto',
              text: '自动建表',
              operations: (
                <ProForm
                  style={{ padding: '0', marginBottom: '-10px' }}
                  fields={[
                    {
                      name: 'tableName',
                      type: 'string',
                      title: '表名称',
                      required: true,
                      defaultValue: 'test_table_name',
                    },
                  ]}
                  onRef={form => (autoCreateTableRef.current = form)}
                  submitter={false}
                />
              ),
            },
            {
              value: 'select',
              text: '选择已有表',
              operations: (
                <ProForm
                  style={{ padding: '0', marginBottom: '-10px' }}
                  fields={[
                    {
                      name: 'tableName',
                      type: 'string',
                      title: 'test',
                      component: 'select',
                      required: true,
                      appearance: 'button',
                      size: 'full',
                      options: [
                        {
                          value: 'test_table_name',
                          text: 'test_table_name',
                        },
                      ],
                      defaultValue: 'test_table_name',
                    },
                  ]}
                  onRef={form => (selectTableRef.current = form)}
                  submitter={false}
                />
              ),
            },
          ],
        });
      },
    },
  ];

  const onSelect = data => {
    setSelectedFields(data);
  };

  const getTargetFields = async (table: string) => {
    //根据选择的table获取字段 todo
    const res = await new Promise(resolve => {
      setTimeout(() => {
        resolve([
          {
            id: 0,
            fieldName: 'test',
            fieldType: 'string',
            fieldComment: '备注',
          },
        ]);
      }, 1000);
    });
    return res;
  };

  //handle add subscrible
  const handleOk = () => {
    formRef.current.submit(values => {
      console.log(values);
    });
    autoCreateTableRef.current &&
      autoCreateTableRef.current.submit(values => {
        console.log(values);
      });
    selectTableRef.current &&
      selectTableRef.current.submit(values => {
        console.log(values);
      });
    console.log(selectedFields);
  };

  return (
    <Drawer
      style={{ width: '100%', zIndex: 1000 }}
      size="l"
      placement="right"
      disableAnimation={false}
      outerClickClosable={true}
      showMask={true}
      visible={visible}
      title="新增订阅"
      footer={
        <div style={{ float: 'right' }}>
          <Button type="primary" onClick={handleOk}>
            新增
          </Button>
          <Button style={{ marginLeft: '10px' }} type="weak" onClick={onClose}>
            取消
          </Button>
        </div>
      }
      onClose={onClose}
    >
      <Row style={{ padding: '0 8%' }} gap={20}>
        <Col span={24}>
          <p style={{ fontSize: '16px', fontWeight: 'bold' }}>配置数据源信息</p>
        </Col>
        <Col span={12}>
          <div style={{ height: '300px', border: '1px solid #E2E2E2', padding: '20px 20px' }}>
            <Form>
              <Form.Title>数据来源</Form.Title>
              <Form.Item label="接入方式">
                <Form.Text>SDK</Form.Text>
              </Form.Item>
              <Form.Item label="单日峰值" tips={'tips文案 todo'}>
                <Form.Text>Tea</Form.Text>
              </Form.Item>
              <Form.Item label="单日最大存储量">
                <Form.Text>1000G</Form.Text>
              </Form.Item>
              <Form.Item label="平均每条数据大小">
                <Form.Text>1GB</Form.Text>
              </Form.Item>
              <Form.Item label="采集类型">
                <Form.Text>UTF-8</Form.Text>
              </Form.Item>
              <Form.Item label="分隔符">
                <Form.Text>分号</Form.Text>
              </Form.Item>
            </Form>
          </div>
        </Col>
        <Col span={12}>
          <div style={{ height: '300px', border: '1px solid #E2E2E2', padding: '20px 20px' }}>
            <Form.Title>写入信息</Form.Title>
            <ProForm
              fields={formFields}
              submitter={false}
              onRef={form => (formRef.current = form)}
              onFormValuesChange={form => {
                //listen form values change
                const tableType = form.getValuesIn('tableType');
                if (tableType === 'auto' || tableType === 'select') {
                  setIsFieldsMapVisible(true);
                }
              }}
            />
          </div>
        </Col>
        {isFieldsMapVisible === true && (
          <>
            <Col span={24}>
              <p style={{ fontSize: '16px', fontWeight: 'bold' }}>字段映射</p>
              <p>选择需要订阅的来源字段，并对齐写入字段，注意：请确定字段为一一映射关系。</p>
            </Col>
            <Col span={24}>
              <FieldsMap
                compact={true}
                bordered={true}
                sourceFields={defaultRecords}
                targetFields={targetFields}
                onSelect={onSelect}
                getTargetFields={getTargetFields}
              />
            </Col>
          </>
        )}
      </Row>
    </Drawer>
  );
};

export default AddSubscribeDrawer;
