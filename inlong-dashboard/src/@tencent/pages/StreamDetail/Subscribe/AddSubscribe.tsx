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
import FieldsMap, { SelectedFieldsType } from '@/@tencent/components/FieldsMap';
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
import { useRequest } from 'ahooks';
import { useProjectId } from '@/@tencent/components/Use/useProject';
import { message } from '@tencent/tea-component';
import ReadonlyForm, { ReadonlyFormFieldItemType } from '@/@tencent/components/ReadonlyForm';
import { SubscribeFormRef, SubscribeFormProps } from './common';
import Clickhouse, { fields as ckFields } from './Clickhouse';
import Hive, { fields as hiveFields } from './Hive';
import Thive, { fields as thiveFields } from './Thive';
// import Hudi, { fields as hudiFields } from './Hudi';
// import Kafka from './Kafka';
import Iceberg from './Iceberg';
import MQ, { MQTypeEnum, fields as MqFields } from './MQ';

export interface AddSubscribeDrawerProps {
  visible: boolean;
  onClose: () => void;
  streamId: string;
  info: Record<string, any>;
  pageType?: 'c' | 'u' | 'r';
  subscribeId?: number;
  subscribeType?: SinkTypeEnum;
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

const AddSubscribeDrawer = ({
  visible,
  onClose,
  streamId,
  info,
  pageType = 'c',
  subscribeId,
  subscribeType,
}: AddSubscribeDrawerProps) => {
  const [projectId] = useProjectId();
  const [targetFields, setTargetFields] = useState<FieldData[]>([]);
  const [selectedFields, setSelectedFields] = useState<SelectedFieldsType>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const formRef = useRef<SubscribeFormRef>();
  const [writeType, setWriteType] = useState(subscribeType || SinkTypeEnum.Hive);

  const { data: savedData, run: getSubscribeData } = useRequest(
    {
      url: `/subscribe/${sinkTypeApiPathMap.get(subscribeType) || 'innermq'}/query`,
      method: 'POST',
      data: {
        projectID: projectId,
        streamID: streamId,
        subscribeID: subscribeId,
      },
    },
    {
      manual: true,
      onSuccess: result => setTargetFields(result.fieldMappings),
    },
  );

  useEffect(() => {
    if (visible && subscribeId) {
      getSubscribeData();
      setWriteType(subscribeType);
    }
  }, [visible, getSubscribeData, subscribeId]);

  const setTargetFieldsPro = useCallback(
    data =>
      setTargetFields(typeof data === 'boolean' ? (data === true ? info.fieldsData : []) : data),
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
        size: 'm',
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
    setTargetFields(info.fieldsData);
  }, [writeType]);

  const onSelect = data => {
    setSelectedFields(data);
  };

  //handle add subscrible
  const handleOk = async () => {
    setLoading(true);
    try {
      const basicFrom = pageType === 'u' ? savedData : await formRef.current?.submit();
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
        streamID: parseInt(streamId, 10),
        inLongGroupID: info.inLongGroupID,
        inLongStreamID: info.inLongStreamID,
        encodeType: info.encodeType,
        dataSeparator: info.dataSeparator,
        fieldMappings,
        ...(basicFrom as object),
      } as Record<string, any>;
      const sinkType: SinkTypeEnum = writeType;
      await request({
        url: `/subscribe/${sinkTypeApiPathMap.get(sinkType)}/${
          pageType === 'c' ? 'create' : 'update'
        }`,
        method: 'POST',
        data,
      });
      message.success({
        content: `${pageType === 'c' ? '新建' : '保存'}订阅成功！`,
      });
      onClose();
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
      title={
        {
          c: '新增订阅',
          u: '编辑订阅',
          r: '订阅详情',
        }[pageType]
      }
      footer={
        <div style={{ float: 'right' }}>
          {pageType === 'r' ? (
            <Button type="weak" onClick={onClose}>
              关闭
            </Button>
          ) : (
            [
              <Button type="primary" onClick={handleOk} loading={loading}>
                {pageType === 'c' ? '新增' : '保存'}
              </Button>,
              <Button style={{ marginLeft: '10px' }} type="weak" onClick={onClose}>
                取消
              </Button>,
            ]
          )}
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
                [SinkTypeEnum.Hive]: [Hive, hiveFields],
                [SinkTypeEnum.Thive]: [Thive, thiveFields],
                [SinkTypeEnum.Clickhouse]: [Clickhouse, ckFields],
                // [SinkTypeEnum.Hudi]: [Hudi, hudiFields],
                // [SinkTypeEnum.Kafka]: [Kafka, []],
                [SinkTypeEnum.Iceberg]: [Iceberg, []],
                [SinkTypeEnum.MQ]: [MQ, MqFields],
              };
              if (pageType === 'r' || pageType === 'u') {
                const fields = (dict[writeType]?.[1] || MqFields) as ReadonlyFormFieldItemType[];
                return (
                  fields && (
                    <ReadonlyForm
                      conf={[
                        {
                          fields: [
                            {
                              label: '写入类型',
                              value: 'subscribeType',
                              enumMap: sinkTypeMap,
                            } as ReadonlyFormFieldItemType,
                          ]
                            .concat(fields)
                            .map(k => ({ ...k, col: 24 })),
                        },
                      ]}
                      defaultValues={savedData}
                    />
                  )
                );
              } else if (visible && pageType === 'c') {
                const Form = dict[writeType]?.[0] as typeof Hive;
                return (
                  Form && (
                    <Form
                      {...props}
                      submitter={false}
                      onFormValuesChange={(form: ProFormIns) => {
                        setWriteType(form.values?.writeType);
                      }}
                      ref={formRef}
                    />
                  )
                );
              }
            })()}
          </div>
        </Col>
        {![SinkTypeEnum.MQ, ...Object.values(MQTypeEnum)].includes(writeType) &&
          (pageType !== 'u' || (pageType === 'u' && savedData?.fieldMappings)) && (
            <>
              <Col span={24}>
                <p style={{ fontSize: '16px', fontWeight: 'bold' }}>字段映射</p>
                <p>选择需要订阅的来源字段，并对齐写入字段，注意：请确定字段为一一映射关系。</p>
              </Col>
              <Col span={24}>
                <FieldsMap
                  readonly={pageType === 'r'}
                  compact={true}
                  bordered={true}
                  sourceFields={info.fieldsData}
                  targetFields={targetFields}
                  defaultSelectFields={
                    pageType === 'u'
                      ? savedData.fieldMappings?.map(item => ({
                          sourceField: { fieldName: item.fieldName, fieldType: item.fieldType },
                          targetField: {
                            fieldName: item.sourceFieldName,
                            fieldType: item.sourceFieldType,
                          },
                        }))
                      : undefined
                  }
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
