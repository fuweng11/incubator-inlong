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

import React, { useRef, useCallback, forwardRef, useImperativeHandle, Ref } from 'react';
import { Button } from '@tencent/tea-component';
import { ProForm, ProFormProps, Form } from '@tencent/tea-material-pro-form';
import { WriteModeEnum, writeModeMap } from '@/@tencent/enums/subscribe/hudi';
import { SubscribeFormProps, SubscribeFormRef } from './common';

const Clickhouse = forwardRef((props: SubscribeFormProps, ref: Ref<SubscribeFormRef>) => {
  const { fields, streamInfo, setTargetFields, ...rest } = props;

  const formRef = useRef<Form>();

  const params: ProFormProps['fields'] = fields.concat([
    {
      name: 'cluster',
      type: 'string',
      title: '数据源',
      required: true,
      component: 'input',
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
      reaction: (fields, values) => {
        // fields.setComponentProps({
        //   suffix: (
        //     <span style={{ color: '#888', display: 'inline-block', marginLeft: '10px' }}>
        //       分区字段：{partitionUnitMap.get(values.partitionUnit)}
        //     </span>
        //   ),
        // });
      },
    },
    {
      name: 'primaryKey',
      type: 'string',
      title: '唯一键',
      tips: 'Upsert写入模式下，需设置唯一键保证数据有序性，支持多选，Append模式则不需要设置唯一键。',
      required: true,
      component: 'input',
      when: values => values.writeMode === WriteModeEnum.Upsert,
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
      name: 'params',
      type: 'string',
      title: '参数',
      component: 'textarea',
      visible: values => values._proConf,
    },
  ]);

  const submit = useCallback(async () => {
    const [basicForm] = await Promise.all([formRef.current.submit() as Record<string, any>]);
    delete basicForm._proConf;
    return basicForm;
  }, []);

  useImperativeHandle(ref, () => ({
    submit,
    fieldsMapProps: {},
  }));

  return <ProForm {...rest} fields={params} onRef={form => (formRef.current = form)} />;
});

export default Clickhouse;
