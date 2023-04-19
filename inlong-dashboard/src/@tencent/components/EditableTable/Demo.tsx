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

import React, { useState, useRef } from 'react';
import { Button, Modal, Input } from '@tencent/tea-component';
import EditableTable, { addons, EditableTableRef } from '.';

export default () => {
  const [visible, setVisible] = useState(false);

  const editableTable = useRef<EditableTableRef>();

  const onOk = async () => {
    const values = await editableTable.current.submit();
    console.log(values);
    setVisible(false);
  };

  return (
    <>
      <Button onClick={() => setVisible(true)}>打开展示可编辑表格</Button>

      <Modal size="l" visible={visible} onClose={() => setVisible(false)}>
        <Modal.Body>
          <EditableTable
            ref={editableTable}
            showRowIndex
            addons={[
              addons.scrollable({
                maxHeight: 310,
              }),
            ]}
            defaultValues={[
              {
                fieldName: '',
                remark: '',
              },
            ]}
            columns={[
              {
                key: 'fieldName',
                header: '字段名',
                rules: {
                  required: '请填写字段名',
                },
                render: ({ field }) => <Input {...field} placeholder="请输入字段名" />,
              },
              {
                key: 'remark',
                header: '备注',
                render: ({ field }) => <Input {...field} placeholder="请输入备注" />,
              },
            ]}
          />
        </Modal.Body>
        <Modal.Footer>
          <Button type="primary" onClick={onOk}>
            确定
          </Button>
        </Modal.Footer>
      </Modal>
    </>
  );
};
