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

import React, { useState, useCallback } from 'react';
import { Checkbox, CheckboxGroupProps, CheckboxProps } from '@tencent/tea-component';

interface CustomCheckboxProps extends CheckboxProps {
  title: string;
}

interface ProCheckboxProps extends CheckboxGroupProps {
  options: CustomCheckboxProps[];
  allOption?: boolean;
}

const ProCheckbox: React.FC<ProCheckboxProps> = ({
  value,
  onChange,
  options,
  allOption = false,
  ...rest
}) => {
  const [innerValue, setInnerValue] = useState<ProCheckboxProps['value']>(value);

  const allOptionObj = {
    title: '全部',
    name: '',
    indeterminate: innerValue.length && innerValue.length !== options.length,
    value: innerValue.length === options.length,
    onChange: (v, ctx) =>
      innerOnChange(
        !innerValue.length || (innerValue.length && innerValue.length !== options.length)
          ? options.map(item => item.name)
          : [],
        ctx,
      ),
  } as CustomCheckboxProps;

  const innerOnChange = useCallback<ProCheckboxProps['onChange']>(
    (value, ctx) => {
      const val =
        allOption && value.includes(allOptionObj.name)
          ? options.filter(item => item.name !== allOptionObj.name).map(item => item.name)
          : value;

      setInnerValue(val);
      if (typeof onChange === 'function') {
        onChange(val, ctx);
      }
    },
    [allOption, allOptionObj.name, options, onChange],
  );

  const innerOptions = allOption ? [allOptionObj].concat(options) : options;

  return (
    <Checkbox.Group value={innerValue} onChange={innerOnChange} {...rest}>
      {innerOptions.map(({ title, ...item }) => (
        <Checkbox key={item.name} {...item}>
          {title}
        </Checkbox>
      ))}
    </Checkbox.Group>
  );
};

export default ProCheckbox;
