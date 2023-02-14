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

import React, { memo } from 'react';
// import { Button, DatePicker, Table, Form, Input } from 'tea-component';
import { PageContainer } from '@/@tencent/components/PageContainer';

const DashBoard = () => {
  // const [form] = Form.useForm();
  // const formRef = useRef<FormInstanceFunctions>();

  // const onSubmit = async (e: SubmitContext) => {
  //   const formValue = formRef.current?.getFieldsValue?.(true) || {};
  //   console.log(formValue);

  //   const test = await formRef.current?.validate();
  //   console.log('validate return: ', test);
  // };

  // const onValueChange = val => {
  //   console.log(val);
  // };

  // const getFormContent = useCallback(
  //   defaultValues => [
  //     {
  //       label: 'keyword',
  //       type: 'input',
  //       name: 'keyword',
  //     },
  //     {
  //       type: 'select',
  //       name: 'type',
  //       label: 'type',
  //       initialValue: defaultValues.type,
  //       props: {
  //         dropdownMatchSelectWidth: false,
  //         options: [
  //           {
  //             key: '1',
  //             value: 'test',
  //           },
  //         ],
  //       },
  //     },
  //   ],
  //   [],
  // );

  // useEffect(() => {
  //   setTimeout(() => {
  //     form.setFieldsValue({
  //       name: 'tde',
  //     });
  //   }, 1000);
  // }, [form]);

  return (
    <PageContainer useDefaultContainer>
      {/* <div>FormGenerator 测试</div>
      <FormGenerator form={form} content={formConfig} /> */}

      {/* <div>Form 测试</div>
      <Form
        form={form}
        ref={formRef}
        onValuesChange={(a, b) => console.log('onValuesChange: ', a, b)}
        layout="inline"
        onSubmit={onSubmit}
      >
        <FormItem label="name" name="name" rules={[{ required: true }]}>
          <Input />
        </FormItem>
        <FormItem label="tel" name="tel">
          <Input />
        </FormItem>
        <FormItem label="deep obj" name={['obj', 'deepKey']}>
          <Input />
        </FormItem>
        <FormItem label="deep obj" name={['obj', 'deepKey2']}>
          <Input />
        </FormItem>
        <FormItem label="deep obj" name={['obj', 'deepKey3']}>
          <DatePicker
            allowInput={false}
            clearable={false}
            defaultValue=""
            enableTimePicker={false}
            format={undefined}
            mode="date"
            placeholder={undefined}
            presetsPlacement="bottom"
          />
        </FormItem>
        <FormItem>
          <Button type="submit">submit</Button>
        </FormItem>
      </Form> */}

      {/* <div>普通 Table 测试</div>
      <Table
        rowKey="id"
        data={[]}
        columns={[
          { colKey: 'applicant', title: 'applicant' },
          { colKey: 'createTime', title: 'createTime' },
        ]}
      /> */}

      {/* <div>HighTable 测试</div>
      <HighTable
        filterForm={{
          content: getFormContent({
            keyword: '',
            pageSize: 5,
            pageNum: 1,
            type: 1,
          }),
        }}
        suffix={<Button type="submit">test</Button>}
        table={{
          rowKey: 'taskId',
          stripe: true,
          data: [
            {
              test: '111',
              time: '2022-07-09',
            },
            {
              test: '111',
              time: '2022-07-09',
            },
          ],
          pagination: {
            total: 2,
            current: 1,
          },
          columns: [
            { colKey: 'test', title: 'test' },
            { colKey: 'time', title: 'time' },
          ],
        }}
      /> */}
    </PageContainer>
  );
};

export default memo(DashBoard);
