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

import React, { createContext, useContext } from 'react';
import styles from './index.module.less';

interface DescriptionType {
  layout?: 'horizontal' | 'vertical';
  title?: string | React.ReactNode;
  // 一行的 DescriptionItems 数量
  column?: number;
  // 额外内容
  extra?: React.ReactNode;
}

interface DescriptionItemType {
  title: string | React.ReactNode;
  // 包含列的数量
  span?: number;
}

type CompoundedComponent = React.FC<DescriptionType> & {
  Item: typeof DescriptionItem;
};

const Context = createContext({ column: 3 });

const Description: CompoundedComponent = ({ title, column, extra, children }) => {
  return (
    <div>
      {(title || extra) && (
        <div className={styles.descriptionTitleContainer}>
          {title && <span className={styles.descriptionTitle}>{title}</span>}
          {extra && <div>{extra}</div>}
        </div>
      )}
      <div className={styles.descriptionContent}>
        <Context.Provider value={{ column }}>{children}</Context.Provider>
      </div>
    </div>
  );
};

const DescriptionItem: React.FC<DescriptionItemType> = ({ title, span = 1, children }) => {
  const context = useContext(Context);
  const column = context.column;

  return (
    <div style={{ flex: `0 0 ${(100 / column) * span}%` }}>
      <span className={styles.descriptionItemTitle}>{title}:</span>
      <span>{children}</span>
    </div>
  );
};

Description.Item = DescriptionItem;

export default Description;
