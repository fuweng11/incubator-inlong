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
import { Select, StatusTip, SelectProps } from '@tencent/tea-component';

export interface FetchSelectProps extends SelectProps {
  request: () => Promise<{ value: string | number; text: string; disabled?: boolean }[]>;
}

const FetchSelect: React.FC<FetchSelectProps> = ({ request, ...rest }) => {
  const [status, setStatus] = useState(null);
  const [options, setOptions] = useState([]);

  const keywordRef = useRef('');

  const fetch = async (inputValue = '') => {
    keywordRef.current = inputValue;

    try {
      setStatus('loading');
      setOptions([]);
      const result = await request();
      if (keywordRef.current === inputValue) {
        setOptions(result);
      }
    } catch (err) {
      setStatus('error');
    } finally {
      setStatus(null);
    }
  };

  return (
    <Select
      style={{ minWidth: 200 }}
      {...rest}
      searchable
      matchButtonWidth
      appearance="button"
      filter={() => true}
      onOpen={fetch}
      onSearch={fetch}
      options={options}
      tips={status && <StatusTip status={status} onRetry={() => fetch(keywordRef.current)} />}
    />
  );
};

export default FetchSelect;
