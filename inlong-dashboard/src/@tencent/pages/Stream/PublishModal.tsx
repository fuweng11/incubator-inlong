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

import React, { useEffect, useMemo } from 'react';
import {
  Modal,
  ModalProps,
  Button,
  Form,
  Table,
  StatusTip,
  Input,
  message,
} from '@tencent/tea-component';
import { useForm, Controller } from 'react-hook-form';
import { useRequest } from 'ahooks';
import { useProjectId } from '@/@tencent/components/Use/useProject';
import request from '@/core/utils/request';
import { dateFormat } from '@/core/utils';
import { SourceTypeEnum, sourceTypeMap, sourceTypeApiPathMap } from '@/@tencent/enums/source';
import {
  encodeTypeMap,
  dataSeparatorMap,
  peakRateMap,
  dataLevelMap,
} from '@/@tencent/enums/stream';
import ReadonlyForm, { ReadonlyFormItemType } from '@/@tencent/components/ReadonlyForm';
import { fields as fileFields } from '@/@tencent/enums/source/file';
import { fields as mysqlFields } from '@/@tencent/enums/source/mysql';
import { fields as postgreSqlFields } from '@/@tencent/enums/source/postgreSql';

interface FieldsParseProps extends ModalProps {
  id?: number | string;
  onOk: (values: Record<string, unknown>) => void;
  sourceType?: SourceTypeEnum;
}

export const getFields = (accessModel): ReadonlyFormItemType[] =>
  [
    {
      title: '基本信息',
      fields: [
        { label: '数据流名称', value: 'name' },
        { label: '数据流ID', value: 'streamID' },
        { label: '创建人', value: 'creator' },
        {
          label: '创建时间',
          value: 'creatTime',
          render: text => text && dateFormat(new Date(text)),
        },
        { label: '数据分级', value: 'dataLevel', enumMap: dataLevelMap },
        { label: '描述', value: 'remark' },
      ],
    },
    {
      title: '接入信息',
      fields: [{ label: '接入方式', value: 'accessModel', enumMap: sourceTypeMap }],
    },
    {
      title: '数据流量',
      fields: [
        { label: '单日峰值', value: 'peakRate', enumMap: peakRateMap },
        { label: '单日最大接入量', value: 'peakTotalSize', unit: 'GB' },
        { label: '单条数据最大值', value: 'msgMaxLength', unit: 'Byte' },
      ],
    },
    accessModel &&
      accessModel !== SourceTypeEnum.SDK && {
        title: '数据源信息',
        fields: {
          [SourceTypeEnum.FILE]: fileFields,
          [SourceTypeEnum.MySQL]: mysqlFields,
          [SourceTypeEnum.PostgreSQL]: postgreSqlFields,
        }[accessModel],
      },
    {
      title: '数据格式',
      fields: [
        { label: '编码类型', value: 'encodeType', enumMap: encodeTypeMap },
        { label: '分隔符', value: 'dataSeparator', enumMap: dataSeparatorMap },
      ],
    },
  ].filter(Boolean);

const FieldsParse: React.FC<FieldsParseProps> = ({
  id,
  sourceType,
  visible,
  onOk,
  onClose,
  ...rest
}) => {
  const [projectId] = useProjectId();

  const { control, formState, reset, handleSubmit } = useForm({
    mode: 'onChange',
    defaultValues: {
      reason: '',
    },
  });

  const { errors } = formState;

  const { data = {}, run } = useRequest(
    {
      url: sourceTypeApiPathMap.get(sourceType)
        ? `/access/${sourceTypeApiPathMap.get(sourceType)}/query`
        : '/access/query/info',
      method: 'POST',
      data: {
        projectID: projectId,
        streamID: id,
      },
    },
    {
      manual: true,
    },
  );

  const accessModel = data.accessModel;

  const conf = useMemo(() => {
    return getFields(accessModel);
  }, [accessModel]);

  const handleOk = handleSubmit(async values => {
    await request({
      url: '/approval/submitApprove',
      method: 'POST',
      data: {
        ...data,
        reason: values.reason,
        projectID: projectId,
      },
    });
    message.success({
      content: (
        <div>
          申请发布上线提交成功，点击
          <a target="_blank" rel="noreferrer" href="/audit?tab=apply">
            查看审批单
          </a>
        </div>
      ),
    });
    onOk(values);
  });

  useEffect(() => {
    if (visible) {
      run();
    } else {
      reset();
    }
  }, [visible, reset, run]);

  return (
    <Modal size="xl" caption="发布上线审批" visible={visible} onClose={onClose} {...rest}>
      <Modal.Body>
        <ReadonlyForm
          conf={conf}
          defaultValues={data}
          footer={
            <>
              <Form.Item label="数据字段" align="middle">
                <Table
                  bordered
                  verticalTop
                  records={data?.fieldsData || []}
                  recordKey="instanceId"
                  columns={[
                    {
                      key: 'fieldName',
                      header: '字段名',
                    },
                    {
                      key: 'fieldType',
                      header: '字段类型',
                    },
                    {
                      key: 'remark',
                      header: '字段描述',
                    },
                  ]}
                  topTip={!data?.fieldsData?.length && <StatusTip status="empty" />}
                />
              </Form.Item>

              <Form.Item
                label="申请原因"
                align="middle"
                required
                status={errors.reason?.message ? 'error' : undefined}
                message={errors.reason?.message}
              >
                <Controller
                  name="reason"
                  control={control}
                  rules={{ validate: value => (value ? undefined : '请填写申请原因') }}
                  render={({ field }) => (
                    <Input.TextArea
                      {...field}
                      style={{ width: '100%' }}
                      placeholder="请输入申请原因，不超过100个字"
                      maxLength={100}
                    />
                  )}
                />
              </Form.Item>
            </>
          }
        />
      </Modal.Body>

      <Modal.Footer>
        <Button type="primary" onClick={handleOk}>
          确定
        </Button>
        <Button type="weak" onClick={onClose}>
          取消
        </Button>
      </Modal.Footer>
    </Modal>
  );
};

export default FieldsParse;
