import React from 'react';
import { Button } from 'antd';
import HighSelect, { HighSelectProps } from '@/components/HighSelect';
import i18n from '@/i18n';

const ProductSelect: React.FC<HighSelectProps> = _props => {
  const props: HighSelectProps = {
    ..._props,
    showSearch: true,
    allowClear: true,
    filterOption: false,
    style: { minWidth: 200, width: 450 },
    options: {
      ..._props.options,
      requestTrigger: ['onOpen', 'onSearch'],
      requestService: productName => ({
        url: '/sc/product/list',
        params: {
          productName,
          pageNum: 1,
          pageSize: 20,
        },
      }),
      requestParams: {
        formatResult: result =>
          result?.map(item => ({
            ...item,
            label: item.name,
            value: item.id,
          })),
      },
    },
    addonAfter: (
      <Button type="link" target="_blank" href="https://tdwsecurity.oa.com/auth/product">
        {i18n.t('components.ProductSelect.Create')}
      </Button>
    ),
  };

  return <HighSelect {...props} />;
};

export default ProductSelect;
