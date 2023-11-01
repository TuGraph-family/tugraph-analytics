import React from 'react';
import {Controlled as CodeMirror} from 'react-codemirror2';
import {useIntl} from "@@/exports";
import 'codemirror/lib/codemirror.js'
import 'codemirror/lib/codemirror.css'
import 'codemirror/theme/idea.css';
import 'codemirror/addon/display/autorefresh'
import 'codemirror/mode/jsx/jsx';

import {ProCard} from "@ant-design/pro-components";
import {Pagination} from "antd";

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
    <CodeMirror
      value={data}
      options={{
        mode: 'jsx',
        theme: 'idea',
        lineNumbers: true,
        readOnly: true,
        lineWiseCopyCut: true,
        autofocus: false,
        lineWrapping: true,
        smartIndent: true,
        lint: true,
        autoRefresh: true,
        gutters: ['CodeMirror-lint-markers'],
      }}
      editorDidMount={(editor) => {
        editor.setSize('auto', '600px');
      }}
      onChange={(editor: any, data: any, value: string) => {
      }}
      onBeforeChange={(editor: any, data: any, value: string) => {
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
