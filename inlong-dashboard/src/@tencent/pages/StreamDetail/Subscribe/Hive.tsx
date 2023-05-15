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

import React, { useState, useRef, useCallback, forwardRef, useImperativeHandle, Ref } from 'react';
import { ProForm, ProFormProps, Form } from '@tencent/tea-material-pro-form';
import ContentRadio from '@/@tencent/components/ContentRadio';
import { partitionUnitMap } from '@/@tencent/enums/subscribe/hive';
import request from '@/core/utils/request';
import { useProjectId } from '@/@tencent/components/Use/useProject';
import { SubscribeFormProps, SubscribeFormRef, FieldData } from './common';

export { fields } from '@/@tencent/enums/subscribe/hive';

const Hive = forwardRef((props: SubscribeFormProps, ref: Ref<SubscribeFormRef>) => {
  const { fields, streamInfo, setTargetFields, ...rest } = props;

  const [projectId] = useProjectId();

  const [selectedTable, setSelectedTable] = useState<string>('');

  const formRef = useRef<Form>();

  const autoCreateTableRef = useRef<Form>();

  const selectTableRef = useRef<Form>();

  const [isCreateTable, setIsCreateTable] = useState<boolean>();

  const getTableList = useCallback(async (db: string) => {
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
        value: 'test',
      },
    ];
  }, []);

  //TODO 暂未提供接口 //根据选择的table获取字段
  const getTargetFields: SubscribeFormRef['getTargetFields'] = useCallback(async () => {
    // const selectedTable
    const res = await new Promise<FieldData[]>(resolve => {
      setTimeout(() => {
        resolve([
          {
            sequence: 55,
            fieldName: 'test',
            fieldType: 'string',
            remark: '备注',
          },
        ]);
      }, 300);
    });
    return res;
  }, []);

  const submit = useCallback(async () => {
    const [basicForm, autoTableForm, selectTableForm] = await Promise.all([
      formRef.current.submit() as Record<string, any>,
      autoCreateTableRef.current?.submit() as object,
      selectTableRef.current?.submit() as object,
    ]);
    return {
      ...basicForm,
      ...(basicForm.isCreateTable ? autoTableForm : selectTableForm),
      partitionInterval: 1,
    };
  }, []);

  useImperativeHandle(ref, () => ({
    submit,
    getTargetFields,
    fieldsMapProps: {
      candoAutoAdd: !isCreateTable,
    },
  }));

  const params: ProFormProps['fields'] = fields.concat([
    {
      name: 'partitionUnit',
      type: 'string',
      title: '分区间隔',
      required: true,
      component: 'radio',
      defaultValue: 'H',
      options: [
        {
          name: 'H',
          title: '小时',
        },
        {
          name: 'D',
          title: '天',
        },
      ],
      reaction: (fields, values) => {
        fields.setComponentProps({
          suffix: (
            <span style={{ color: '#888', display: 'inline-block', marginLeft: '10px' }}>
              分区字段：{partitionUnitMap.get(values.partitionUnit)}
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
      size: 'm',
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
        field.setValue(data[0]?.dbName);
      },
      suffix: (
        <a href="/" target="_blank">
          申请资源
        </a>
      ),
    },
    {
      name: 'isCreateTable',
      type: 'string',
      title: '数据来源',
      required: true,
      component: ContentRadio,
      defaultValue: true,
      reaction: async (field, values) => {
        setIsCreateTable(values.isCreateTable);
        if (streamInfo?.fieldsData) {
          if (values.isCreateTable) {
            setTargetFields(true);
          } else {
            const res = await getTargetFields();
            setTargetFields(res);
          }
        }

        const tableList = await getTableList(values.database);
        field.setComponentProps({
          radios: [
            {
              value: true,
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
                      defaultValue: streamInfo.name,
                    },
                  ]}
                  onRef={form => (autoCreateTableRef.current = form)}
                  submitter={false}
                />
              ),
            },
            {
              value: false,
              text: '选择已有表',
              // disabled: true,
              operations: (
                <ProForm
                  style={{ padding: '0', marginBottom: '-10px' }}
                  fields={[
                    {
                      name: 'tableName',
                      type: 'string',
                      title: '表名称',
                      component: 'select',
                      required: true,
                      appearance: 'button',
                      size: 'm',
                      options: tableList.map(item => ({ text: item.text, value: item.value })),
                      defaultValue: tableList[0]?.value,
                      reaction: async (field, values: any) => {
                        setSelectedTable(values.tableName);
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
  ]);

  return <ProForm {...rest} fields={params} onRef={form => (formRef.current = form)} />;
});

export default Hive;
