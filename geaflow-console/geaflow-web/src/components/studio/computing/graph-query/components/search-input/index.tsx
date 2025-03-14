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

import type { ChangeEvent } from "react";
import React from "react";
import type { AutoCompleteProps, InputProps } from "antd";
import { AutoComplete, Input } from "antd";
import { useImmer } from "use-immer";
import IconFont from "../icon-font";
import { PUBLIC_PERFIX_CLASS } from "../../../constant";

import styles from "./index.module.less";

interface SearchProps extends InputProps {
  onSearch?: (keyword: string) => void;
  autoCompleteProps?: AutoCompleteProps;
}

const SearchInput: React.FC<SearchProps> = ({
  onSearch,
  onChange,
  width,
  style,
  defaultValue,
  value,
  autoCompleteProps,
  ...others
}) => {
  const [state, setState] = useImmer<{ keyword: any }>({
    keyword: defaultValue || value || "",
  });
  const handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    if (onChange) {
      onChange(e);
    }
    setState((draft) => {
      draft.keyword = e.target.value;
    });
  };
  const onAutoCompleteChange = (inputValue: string) => {
    setState((draft) => {
      draft.keyword = inputValue;
    });
  };
  const onInputSearch = () => {
    if (onSearch) {
      onSearch(state.keyword);
    }
  };
  return (
    <div className={styles[`${PUBLIC_PERFIX_CLASS}-search-ipt`]}>
      <AutoComplete {...autoCompleteProps} onChange={onAutoCompleteChange}>
        <Input
          style={{ width, ...style }}
          bordered={false}
          suffix={
            <IconFont
              type="icon-sousuo"
              onClick={onInputSearch}
              style={{ fontSize: "18px" }}
            />
          }
          allowClear
          {...others}
          onChange={handleInputChange}
          value={state.keyword}
          onPressEnter={onInputSearch}
        />
      </AutoComplete>
    </div>
  );
};

export default SearchInput;
