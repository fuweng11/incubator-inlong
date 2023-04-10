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

import React, { useState } from 'react';
import { Drawer, Button, Form } from '@tencent/tea-component';
import FieldsParseDemo from '@/@tencent/components/FieldsParse/Demo';
import ProCheckboxDemo from '@/@tencent/components/ProCheckbox/Demo';

export default () => {
  const [visible, setVisible] = useState(false);

  return (
    <>
      <Button
        onClick={() => setVisible(true)}
        style={{
          width: 48,
          height: 48,
          position: 'fixed',
          padding: 0,
          right: 40,
          bottom: 100,
          zIndex: 100,
          borderRadius: '50%',
        }}
      >
        DEV
      </Button>

      <Drawer visible={visible} onClose={() => setVisible(false)}>
        <Form>
          <Form.Item label="FieldsParse">
            <FieldsParseDemo />
          </Form.Item>

          <Form.Item label="ProCheckbox">
            <ProCheckboxDemo />
          </Form.Item>
        </Form>
      </Drawer>
    </>
  );
};
