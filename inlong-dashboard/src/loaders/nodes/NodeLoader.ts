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

import { NodeDefaultLoader } from './NodeDefaultLoader';
import { MetaExportWithBackendList } from '@/plugins/types';

export class NodeLoader extends NodeDefaultLoader {
  // You can extends NodeLoader at here...
  loadPluginList<T>(
    defaultsList: MetaExportWithBackendList<T>,
    extendsList: MetaExportWithBackendList<T>,
  ): MetaExportWithBackendList<T> {
    const defaultsValueToExtValue = (defaultsValue: string) => {
      const specMaps = {
        CLICKHOUSE: 'INNER_CK',
      };
      if (specMaps[defaultsValue]) return specMaps[defaultsValue];
      return `INNER_${defaultsValue}`;
    };

    const extendsListMap = new Map(extendsList.map(item => [item.value, item]));
    let output = defaultsList.map(item => {
      const extValue = defaultsValueToExtValue(item.value);
      if (extendsListMap.get(extValue)) {
        const extItem = extendsListMap.get(extValue);
        extendsListMap.delete(extValue);
        return {
          ...extItem,
          label: item.label,
        };
      }
      return item;
    });
    extendsListMap.forEach(item => output.push(item));
    return output;
  }
}
