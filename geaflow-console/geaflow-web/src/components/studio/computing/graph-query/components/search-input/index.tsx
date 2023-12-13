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
