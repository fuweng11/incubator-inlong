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
import { Select, TagSelect, StatusTip, SelectProps, TagSelectProps } from '@tencent/tea-component';
import debounce from 'lodash/debounce';

interface BaseProps {
  request: (inputValue?: string) => Promise<SelectProps['options']>;
  trigger?: ('open' | 'search')[];
  isMultiple?: boolean;
}

export interface FetchSelectProps extends SelectProps, BaseProps {}

export interface FetchTagSelectProps extends TagSelectProps, BaseProps {}

const FetchSelect: React.FC<FetchSelectProps | FetchTagSelectProps> = ({
  request,
  trigger = ['open', 'search'],
  isMultiple = false,
  ...rest
}) => {
  const [status, setStatus] = useState(null);

  const [options, setOptions] = useState<SelectProps['options']>(
    Array.isArray(rest.value)
      ? rest.value.map(item => ({ value: item }))
      : rest.value
      ? [{ value: rest.value }]
      : [],
  );

  const keywordRef = useRef('');

  const fetch = debounce(async (inputValue = '') => {
    keywordRef.current = inputValue;

    try {
      setStatus('loading');
      setOptions([]);
      const result = await request(inputValue);
      if (keywordRef.current === inputValue) {
        if (!result.length) setStatus('empty');
        setOptions(result);
      }
    } catch (err) {
      setStatus('error');
    } finally {
      setStatus(prev => (prev === 'loading' ? null : prev));
    }
  }, 300);

  const props: SelectProps | TagSelectProps = {
    style: { minWidth: 200 },
    searchPlaceholder: '请输入关键字搜索',
    ...rest,
    searchable: true,
    matchButtonWidth: true,
    appearance: 'button',
    filter: () => true,
    onOpen: trigger.includes('open') ? fetch : undefined,
    onSearch: trigger.includes('search') ? fetch : undefined,
    options: options,
    tips: status && <StatusTip status={status} onRetry={() => fetch(keywordRef.current)} />,
  };

  if (isMultiple) {
    return <TagSelect optionsOnly placeholder="请输入关键字搜索" {...(props as TagSelectProps)} />;
  }

  return <Select {...(props as SelectProps)} />;
};

export default FetchSelect;
