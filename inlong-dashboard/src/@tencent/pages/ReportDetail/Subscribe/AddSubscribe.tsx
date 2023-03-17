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
import FieldsMap, { selectedFieldsProps } from '@/@tencent/components/FieldsMap';
import { FieldData } from '@/@tencent/components/FieldsMap';
import { Drawer, Button, Row, Col, Form } from '@tencent/tea-component';
import { ProForm, ProFormProps } from '@tencent/tea-material-pro-form';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  accessTypeMap,
  encodeTypeMap,
  dataSeparatorMap,
  peakRateMap,
} from '@/@tencent/enums/stream';
import request from '@/utils/request';
import { useProjectId } from '@/@tencent/components/Use/useProject';
import { message } from '@tencent/tea-component';

export interface AddSubscribeDrawerProps {
  visible: boolean;
  onClose: () => void;
  streamId: string;
  refreshSubscribeList: () => any;
  info: Record<string, any>;
}

const insertIndex = (data: Array<any>) =>
  data ? data.map((item, index) => ({ ...item, id: index })) : [];

const AddSubscribeDrawer = ({
  visible,
  onClose,
  streamId,
  refreshSubscribeList,
  info,
}: AddSubscribeDrawerProps) => {
  const [projectId] = useProjectId();
  const [targetFields, setTargetFields] = useState<FieldData>([]);
  const [selectedFields, setSelectedFields] = useState<selectedFieldsProps>([]);
  const [isFieldsMapVisible, setIsFieldsMapVisible] = useState<boolean>(false);
  const [isCreateTable, setIsCreateTable] = useState<boolean>(false);
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [loading, setLoading] = useState<boolean>(false);
  const formRef = useRef<any>();
  const autoCreateTableRef = useRef<any>();
  const selectTableRef = useRef<any>();

  //get table
  const getTableList = useCallback(async (db: string) => {
    try {
      //todo 接口暂未提供
      // const res = await request({
      //   url: '/access/querytablelist',
      //   method: 'POST',
      //   data: {
      //     db,
      //   },
      // });
      //todo 待替换
      return [
        {
          text: 'test',
          value: '1',
        },
      ];
    } catch (e) {
      console.warn(e);
    }
  }, []);

  useEffect(() => {
    const fn = async () => {
      if (info && info.fieldsData) {
        if (isCreateTable) {
          setTargetFields(insertIndex(info.fieldsData));
        } else {
          const res = await getTargetFields(selectedTable);
          setTargetFields(res);
        }
      }
    };
    fn();
  }, [isCreateTable]);

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
      name: 'partitionUnit',
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
              分区字段：{values.partitionUnit}
            </span>
          ),
        });
      },
    },
    {
      name: 'dbName',
      type: 'string',
      title: '数据库名称',
      required: true,
      component: 'select',
      appearance: 'button',
      style: { width: '80%' },
      reaction: async (field, values) => {
        const data = await request({
          url: '/project/database/list',
          method: 'POST',
          data: {
            projectID: projectId,
          },
        });
        field.setComponentProps({
          options: data.map(item => ({ text: item.dbName, value: item.dbName })),
        });
        values.dbName = data[0].dbName;
      },
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
      reaction: async (field, values) => {
        setIsCreateTable(values.tableType === 'auto');
        const tableList = await getTableList(values.database);
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
                      name: 'dbTable',
                      type: 'string',
                      title: '表名称',
                      required: true,
                      defaultValue: info.name,
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
              disabled: true,
              operations: (
                <ProForm
                  style={{ padding: '0', marginBottom: '-10px' }}
                  fields={[
                    {
                      name: 'dbTable',
                      type: 'string',
                      title: '表名称',
                      component: 'select',
                      required: true,
                      appearance: 'button',
                      size: 'full',
                      options: tableList.map(item => ({ text: item.text, value: item.value })),
                      defaultValue: tableList[0]?.value,
                      reaction: async (field, values: any) => {
                        setSelectedTable(values.dbTable);
                      },
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

  //TODO 暂未提供接口 //根据选择的table获取字段
  const getTargetFields = async (table?: string): Promise<FieldData> => {
    const res = await new Promise<FieldData>(resolve => {
      setTimeout(() => {
        resolve([
          {
            id: 0,
            sequence: 55,
            fieldName: 'test',
            fieldType: 'string',
            remark: '备注',
          },
        ]);
      }, 1000);
    });
    return res;
  };

  //handle add subscrible
  const handleOk = async () => {
    setLoading(true);
    const [basicFrom, tableForm] = await Promise.all([
      formRef.current.submit(),
      autoCreateTableRef.current.submit(),
      // selectTableRef.current.submit(),
    ]);
    const fieldMappings = selectedFields.map(item => ({
      fieldName: item.targetField.fieldName,
      fieldType: item.targetField.fieldType,
      remark: item.targetField.remark,
      sourceFieldName: item.sourceField.fieldName,
      sourceFieldType: item.sourceField.fieldType,
      sequence: item.targetField.id,
    }));
    const data = {
      projectID: projectId,
      streamID: streamId,
      inLongGroupID: info.inLongGroupID,
      inLongStreamID: info.inLongStreamID,
      encodeType: info.encodeType,
      dataSeparator: info.dataSeparator,
      dbName: basicFrom.dbName,
      dbTable: tableForm.dbTable,
      isCreateTable,
      partitionUnit: basicFrom.partitionUnit,
      partitionInterval: 1,
      fieldMappings,
    };
    try {
      await request({
        url: '/subscribe/thive/create',
        method: 'POST',
        data,
      });
      message.success({
        content: '新建订阅成功！',
      });
      onClose();
      refreshSubscribeList();
    } catch (e) {
      console.warn(e);
    } finally {
      setLoading(false);
    }
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
          <Button type="primary" onClick={handleOk} loading={loading}>
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
                <Form.Text>{accessTypeMap.get(info.accessModel) || info.accessModel}</Form.Text>
              </Form.Item>
              <Form.Item
                label="单日峰值"
                tips="单日峰值（条/秒），请按照数据实际上报量以及后续增长情况，适当上浮20%-50%左右填写，用于容量管理。如果上涨超过容量限制，平台在紧急情况下会抽样或拒绝数据"
              >
                <Form.Text>{peakRateMap.get(info.peakRate) || info.peakRate}</Form.Text>
              </Form.Item>
              <Form.Item label="单日最大存储量">
                <Form.Text>{info.peakTotalSize} GB</Form.Text>
              </Form.Item>
              <Form.Item label="平均每条数据大小">
                <Form.Text>{info.msgMaxLength} GB</Form.Text>
              </Form.Item>
              <Form.Item label="采集类型">
                <Form.Text>{encodeTypeMap.get(info.encodeType) || info.encodeType}</Form.Text>
              </Form.Item>
              <Form.Item label="分隔符">
                <Form.Text>
                  {dataSeparatorMap.get(info.dataSeparator) || info.dataSeparator}
                </Form.Text>
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
                candoAutoAdd={!isCreateTable}
                compact={true}
                bordered={true}
                sourceFields={insertIndex(info.fieldsData)}
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
