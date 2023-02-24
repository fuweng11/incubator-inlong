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
import { Radio } from '@tencent/tea-component';
import React from 'react';

type radios = {
  value: string;
  text: string;
  operations?: string | React.ReactNode;
}[];

export interface ContentRadioProps {
  value?: string;
  radios: radios;
  onChange?: any;
  style?: React.CSSProperties;
}

const ContentRadio = ({ radios, value, ...props }: ContentRadioProps) => {
  return (
    <Radio.Group layout="column" value={value} {...props}>
      {radios.map(radio => (
        <div
          style={{
            background: '#f2f2f2',
            padding: '10px 5px',
            marginBottom: '10px',
            position: 'relative',
            top: '-10px',
          }}
          key={radio.value}
        >
          <Radio name={radio.value} key={radio.value}>
            <div style={{ marginBottom: '5px' }}>{radio.text}</div>
            {value == radio.value && radio.operations}
          </Radio>
        </div>
      ))}
    </Radio.Group>
  );
};

export default ContentRadio;
