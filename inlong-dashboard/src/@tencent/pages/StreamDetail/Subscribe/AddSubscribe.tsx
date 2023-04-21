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
import FieldsMap, { selectedFieldsProps } from '@/@tencent/components/FieldsMap';
import { FieldData } from '@/@tencent/components/FieldsMap';
import { Drawer, Button, Row, Col, Form } from '@tencent/tea-component';
import { Form as ProFormIns, ProFormProps } from '@tencent/tea-material-pro-form';
import React, { useRef, useState } from 'react';
import {
  accessTypeMap,
  encodeTypeMap,
  dataSeparatorMap,
  peakRateMap,
} from '@/@tencent/enums/stream';
import { SinkTypeEnum, sinkTypeMap } from '@/@tencent/enums/subscribe/_basic';
import request from '@/core/utils/request';
import { useProjectId } from '@/@tencent/components/Use/useProject';
import { message } from '@tencent/tea-component';
import { SubscribeFormRef } from './common';
import Clickhouse from './Clickhouse';
import Hive from './Hive';
import Hudi from './Hudi';

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
  const [loading, setLoading] = useState<boolean>(false);
  const formRef = useRef<SubscribeFormRef>();
  const [writeType, setWriteType] = useState(SinkTypeEnum.Hive);

  const props = {
    fields: [
      {
        name: 'writeType',
        type: 'string',
        title: '写入类型',
        required: true,
        component: 'select',
        appearance: 'button',
        style: { width: '80%' },
        defaultValue: writeType,
        options: Array.from(sinkTypeMap).map(([key, ctx]) => ({
          value: key,
          text: ctx,
        })),
      },
    ] as ProFormProps['fields'],
    streamInfo: info,
    setTargetFields,
  };

  const onSelect = data => {
    setSelectedFields(data);
  };

  //handle add subscrible
  const handleOk = async () => {
    setLoading(true);
    try {
      const basicFrom = await formRef.current.submit();
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
        fieldMappings,
        ...(basicFrom as object),
      };
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
          <div style={{ border: '1px solid #E2E2E2', padding: '20px 20px' }}>
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
                <Form.Text>{info.msgMaxLength} Byte</Form.Text>
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
          <div style={{ border: '1px solid #E2E2E2', padding: '20px 20px' }}>
            <Form.Title>写入信息</Form.Title>
            {(() => {
              const dict = {
                [SinkTypeEnum.Hive]: Hive,
                [SinkTypeEnum.Clickhouse]: Clickhouse,
                [SinkTypeEnum.Hudi]: Hudi,
              };
              const Form = dict[writeType];
              return (
                <Form
                  {...props}
                  submitter={false}
                  onFormValuesChange={(form: ProFormIns) => {
                    setTargetFields([]);
                    setWriteType(form.values?.writeType);
                  }}
                  ref={formRef}
                />
              );
            })()}
          </div>
        </Col>
        <Col span={24}>
          <p style={{ fontSize: '16px', fontWeight: 'bold' }}>字段映射</p>
          <p>选择需要订阅的来源字段，并对齐写入字段，注意：请确定字段为一一映射关系。</p>
        </Col>
        <Col span={24}>
          <FieldsMap
            compact={true}
            bordered={true}
            sourceFields={insertIndex(info.fieldsData)}
            targetFields={targetFields}
            onSelect={onSelect}
            getTargetFields={formRef.current?.getTargetFields}
            {...formRef.current?.fieldsMapProps}
          />
        </Col>
      </Row>
    </Drawer>
  );
};

export default AddSubscribeDrawer;
