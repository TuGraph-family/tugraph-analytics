import React from 'react';
import {useIntl} from "@@/exports";
import AceEditor from "react-ace";
import "ace-builds/src-noconflict/mode-jsx";
import "ace-builds/src-noconflict/theme-xcode";
import "ace-builds/src-noconflict/ext-language_tools";
import "ace-builds/src-noconflict/ext-searchbox";

import {ProCard} from "@ant-design/pro-components";
import {Pagination} from "antd";
import {Ace} from "ace-builds";

const FileContentCard: React.FC<{
  title: string,
  subtitle: string,
  data: string,
  totalBytes: number,
  paginationRequestRef: React.MutableRefObject<API.PageRequest>,
  extra: React.ReactNode,
  handlePaginationChange: (page: number, pageSizeKB: number) => void
}> = ({title, subtitle, data, totalBytes, extra, paginationRequestRef, handlePaginationChange}) => {

  const intl = useIntl();

  return <ProCard
    title={title}
    subTitle={subtitle}
    extra={extra}
  >
    <AceEditor
      key={paginationRequestRef?.current?.pageNo}
      value={data}
      mode="jsx"
      theme="xcode"
      name="log_view"
      readOnly={true}
      onLoad={(editor: Ace.Editor) => {
        editor.moveCursorTo(0, 0);
      }}
      wrapEnabled={true}
      showPrintMargin={false}
      showGutter={true}
      highlightActiveLine={true}
      style={{width: 'auto', height: '600px'}}
      setOptions={{
        enableBasicAutocompletion: false,
        enableLiveAutocompletion: false,
        enableSnippets: true,
        showLineNumbers: true,
      }}
    />
    <Pagination current={paginationRequestRef.current.pageNo}
                total={Math.ceil(totalBytes / 1024.0)}
                onChange={handlePaginationChange}
                pageSize={paginationRequestRef.current.pageSize}
                pageSizeOptions={[10, 50, 100]}
                showSizeChanger={true}
                showQuickJumper
                style={{float: 'right'}}
                locale={{
                  items_per_page: intl.formatMessage({id: 'pages.pagination.kb_per_page', defaultMessage: 'KB / Page'}),
                }}
    />
  </ProCard>
}


export default FileContentCard;
