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

import React, {
  useRef,
  useCallback,
  forwardRef,
  useImperativeHandle,
  Ref,
  useState,
  useEffect,
} from 'react';
import { Button } from '@tencent/tea-component';
import { ProForm, ProFormProps, Form } from '@tencent/tea-material-pro-form';
import { WriteModeEnum, writeModeMap } from '@/@tencent/enums/subscribe/iceberg';
import { SubscribeFormProps, SubscribeFormRef } from './common';
import request from '@/core/utils/request';
import { useProjectId } from '@/@tencent/components/Use/useProject';
import isEmpty from 'lodash/isEmpty';

export { fields } from '@/@tencent/enums/subscribe/iceberg';

const Iceberg = forwardRef((props: SubscribeFormProps, ref: Ref<SubscribeFormRef>) => {
  const { fields, streamInfo, setTargetFields, initialValues, ...rest } = props;
  const [clusterOptions, setClusterOptions] = useState<any[]>([]);

  const formRef = useRef<Form>();

  const [projectId] = useProjectId();

  useEffect(() => {
    if (initialValues) {
      // 编辑状态
      let properties = '';
      let _proConf = false;
      if (!isEmpty(initialValues.properties)) {
        _proConf = true;
        let arr = [];
        Object.keys(initialValues.properties).forEach(k => {
          arr.push(`${k}=${initialValues.properties[k]}`);
        });
        properties = arr.join('\n');
      }
      formRef.current?.setValues({
        inLongNodeName: initialValues.inLongNodeName,
        dbName: initialValues.dbName,
        tableName: initialValues.tableName,
        writeMode: initialValues.writeMode,
        primaryKey: initialValues.primaryKey,
        properties,
        _proConf,
      });
    }
  }, [clusterOptions, initialValues]);

  const params: ProFormProps['fields'] = fields.concat([
    {
      name: 'inLongNodeName',
      type: 'string',
      title: '数据源',
      required: true,
      component: 'select',
      appearance: 'button',
      size: 'm',
      reaction: async (field, values) => {
        const data = await request({
          url: '/datasource/search',
          method: 'POST',
          data: {
            projectID: projectId,
            type: 'ICEBERG',
            pageSize: 99,
            pageNum: 0,
          },
        });
        setClusterOptions(data?.records);
        field.setComponentProps({
          options: (data?.records || []).map(item => ({
            text: item.displayName,
            value: item.name,
          })),
        });
      },
    },
    {
      name: 'dbName',
      type: 'string',
      title: '库',
      required: true,
      component: 'input',
    },
    {
      name: 'tableName',
      type: 'string',
      title: '表',
      required: true,
      component: 'input',
    },
    {
      name: 'writeMode',
      type: 'string',
      title: '写入模式',
      tips: (
        <>
          <div>Append：追加写入。</div>
          <div>
            Upsert：以 upsert 方式插入消息，设置后消息仅只能被消费端处理一次以保证 Exactly-Once。
          </div>
        </>
      ),
      required: true,
      component: 'radio',
      defaultValue: WriteModeEnum.Append,
      options: Array.from(writeModeMap).map(([key, ctx]) => ({ name: key, title: ctx })),
    },
    {
      name: 'primaryKey',
      type: 'string',
      title: '唯一键',
      tips: 'Upsert写入模式下，需设置唯一键保证数据有序性，支持多选，Append模式则不需要设置唯一键。',
      required: false,
      component: 'input',
      // when: values => values.writeMode === WriteModeEnum.Upsert,
    },
    {
      name: '_proConf',
      type: null,
      title: (
        <Button
          type="link"
          onClick={() => {
            if (formRef.current) {
              const { _proConf } = formRef.current.values || {};
              formRef.current?.setValues({ _proConf: !_proConf });
            }
          }}
        >
          高级设置
        </Button>
      ),
      component: () => null,
    },
    {
      name: 'properties',
      type: 'string',
      title: '参数',
      placeholder: '请输入参数名称及值（格式为：parameter=value），多个参数使用换行符分割',
      component: 'textarea',
      visible: values => values._proConf,
    },
  ]);

  const submit = useCallback(async () => {
    const [basicForm] = await Promise.all([formRef.current.submit() as Record<string, any>]);
    // 格式化参数为object
    if (basicForm.properties) {
      let newObject = {};
      basicForm.properties.split('\n').forEach((row: string) => {
        const arr = row.split('=');
        if (arr.length > 1) {
          newObject[arr[0]] = arr[1];
        }
      });
      basicForm.properties = newObject;
    }
    delete basicForm._proConf;
    return basicForm;
  }, []);

  useImperativeHandle(ref, () => ({
    submit,
    fieldsMapProps: {},
  }));

  return <ProForm {...rest} fields={params} onRef={form => (formRef.current = form)} />;
});

export default Iceberg;
