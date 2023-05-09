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
import React, { useRef, useState, useMemo, useCallback, useEffect } from 'react';
import { encodeTypeMap, dataSeparatorMap, peakRateMap } from '@/@tencent/enums/stream';
import { sourceTypeMap, SourceTypeEnum } from '@/@tencent/enums/source';
import type { FieldItemType } from '@/@tencent/enums/source/common';
import { fields as fileFields } from '@/@tencent/enums/source/file';
import { fields as mysqlFields } from '@/@tencent/enums/source/mysql';
import { fields as postgreSqlFields } from '@/@tencent/enums/source/postgreSql';
import { SinkTypeEnum, sinkTypeMap, sinkTypeApiPathMap } from '@/@tencent/enums/subscribe';
import request from '@/core/utils/request';
import { useProjectId } from '@/@tencent/components/Use/useProject';
import { message } from '@tencent/tea-component';
import { SubscribeFormRef, SubscribeFormProps } from './common';
import Clickhouse from './Clickhouse';
import Hive from './Hive';
import Thive from './Thive';
import Hudi from './Hudi';
import Kafka from './Kafka';
import MQ from './MQ';

export interface AddSubscribeDrawerProps {
  visible: boolean;
  onClose: () => void;
  streamId: string;
  refreshSubscribeList: () => any;
  info: Record<string, any>;
}

export const getFields = (accessModel): FieldItemType[] => [
  { label: '接入方式', value: 'accessModel', enumMap: sourceTypeMap },
  { label: '单日峰值', value: 'peakRate', enumMap: peakRateMap },
  { label: '单日最大接入量', value: 'peakTotalSize', unit: 'GB' },
  { label: '单条数据最大值', value: 'msgMaxLength', unit: 'Byte' },
  { label: '编码类型', value: 'encodeType', enumMap: encodeTypeMap },
  { label: '分隔符', value: 'dataSeparator', enumMap: dataSeparatorMap },
  ...(accessModel && accessModel !== SourceTypeEnum.SDK
    ? {
        [SourceTypeEnum.FILE]: fileFields,
        [SourceTypeEnum.MySQL]: mysqlFields,
        [SourceTypeEnum.PostgreSQL]: postgreSqlFields,
      }[accessModel]
    : []),
];

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

  const setTargetFieldsPro = useCallback(
    data =>
      setTargetFields(
        typeof data === 'boolean' ? (data === true ? insertIndex(info.fieldsData) : []) : data,
      ),
    [info.fieldsData],
  );

  const props: SubscribeFormProps = {
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
    setTargetFields: setTargetFieldsPro,
  };

  useEffect(() => {
    console.log('watch writeType', writeType);
    setTargetFields(insertIndex(info.fieldsData));
  }, [writeType]);

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
      const sinkType: SinkTypeEnum = basicFrom.writeType;
      await request({
        url: `/subscribe/${sinkTypeApiPathMap.get(sinkType)}/create`,
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

  const conf = useMemo(() => {
    return getFields(info.accessModel);
  }, [info.accessModel]);

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
              {conf.map(item => (
                <Form.Item label={item.label}>
                  <Form.Text>
                    {(() => {
                      if (item.render) {
                        return item.render(info?.[item.value], item);
                      }
                      const text = item.enumMap
                        ? item.enumMap.get(info?.[item.value]) || info?.[item.value]
                        : info?.[item.value];
                      const unit = item.unit;
                      return unit ? `${text} ${unit}` : text;
                    })()}
                  </Form.Text>
                </Form.Item>
              ))}
            </Form>
          </div>
        </Col>
        <Col span={12}>
          <div style={{ border: '1px solid #E2E2E2', padding: '20px 20px' }}>
            <Form.Title>写入信息</Form.Title>
            {(() => {
              const dict = {
                [SinkTypeEnum.Hive]: Hive,
                [SinkTypeEnum.Thive]: Thive,
                [SinkTypeEnum.Clickhouse]: Clickhouse,
                [SinkTypeEnum.Hudi]: Hudi,
                [SinkTypeEnum.Kafka]: Kafka,
                [SinkTypeEnum.MQ]: MQ,
              };
              const Form = dict[writeType];
              return (
                <Form
                  {...props}
                  submitter={false}
                  onFormValuesChange={(form: ProFormIns) => {
                    setWriteType(form.values?.writeType);
                  }}
                  ref={formRef}
                />
              );
            })()}
          </div>
        </Col>
        {writeType !== SinkTypeEnum.MQ && (
          <>
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
          </>
        )}
      </Row>
    </Drawer>
  );
};

export default AddSubscribeDrawer;
