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

import { DataWithBackend } from '@/metas/DataWithBackend';
import UserSelect from '@/components/UserSelect';
import { ClusterInfo } from '../common/ClusterInfo';

const { I18n, FormField } = DataWithBackend;

export default class SortHiveCluster extends ClusterInfo implements DataWithBackend {
  @FormField({
    type: 'input',
    props: {
      placeholder: 'hdfs://xx-xxx-xx/xxx/xxx',
    },
  })
  @I18n('meta.Clusters.Sort.BackupDataPath')
  backupDataPath: string;

  @FormField({
    type: UserSelect,
  })
  @I18n('meta.Clusters.Sort.BackupHadoopProxyUser')
  backupHadoopProxyUser: string;
}
