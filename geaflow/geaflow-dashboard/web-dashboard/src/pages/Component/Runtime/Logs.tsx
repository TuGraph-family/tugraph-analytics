import React, {useEffect, useRef, useState} from 'react';
import {ActionType} from "@ant-design/pro-components";
import {ProTable} from "@ant-design/pro-table/lib";
import {FormattedMessage} from "@umijs/max";
import {logList} from "@/services/jobs/api";
import RuntimeLayout from "@/pages/Component/Runtime/Runtime";
import {history, useIntl, useParams} from "@@/exports";
import {fetchComponentInfo, formatFileSize, parseAgentUrl, sortTable} from "@/util/CommonUtil";
import {getColumnSearchProps} from "@/util/TableUtil";
import {InputRef} from "antd";

const LogTable: React.FC<{ componentName: string, agentUrl: string | undefined }> = ({
                                                                                       componentName,
                                                                                       agentUrl
                                                                                     }) => {
  const [searchText, setSearchText] = useState('');
  const [searchedColumn, setSearchedColumn] = useState('');
  const searchInput = useRef<InputRef>(null);
  const actionRef = useRef<ActionType>();
  const intl = useIntl();

  return <>
    <ProTable<API.FileInfo>
      headerTitle={intl.formatMessage({
        id: 'pages.components.logs.table.description',
        defaultMessage: 'Log Files'
      })}
      actionRef={actionRef}
      rowKey="path"
      search={false}
      params={{agentUrl}}
      // @ts-ignore
      request={agentUrl == null ? null : async (params: {
        agentUrl: string;
      }) => {
        return logList(params.agentUrl);
      }}
      columns={
        [
          {
            title: <FormattedMessage id="pages.components.logs.table.path" defaultMessage="Path"/>,
            dataIndex: 'path',
            sorter: (a, b) => sortTable(a.path, b.path),
            ...getColumnSearchProps<API.FileInfo>('path', searchText, searchedColumn, searchInput, setSearchText, setSearchedColumn),
            render: (dom, entity) => {
              return (
                <a
                  onClick={() => { // @ts-ignore
                    history.push('/components/' + encodeURIComponent(componentName) + '/logs/' + encodeURIComponent(entity?.path))
                  }}
                >
                  {dom}
                </a>
              );
            },
          },
          {
            title: <FormattedMessage id="pages.components.logs.table.size" defaultMessage="Size"/>,
            dataIndex: 'size',
            sorter: (a, b) => sortTable(a.size, b.size),
            render: (dom, entity) => {
              return (formatFileSize(entity.size));
            },
          }
        ]
      }
    />
  </>
}

const LogsPage: React.FC = () => {
  const [agentUrl, setAgentUrl]: [string | undefined, any] = useState(undefined);

  const pathParams = useParams();
  const componentName = pathParams.componentName;
  if (componentName == null || componentName == 'undefined') {
    history.push("/error");
    return null;
  }

  const intl = useIntl();

  const description = intl.formatMessage({
    id: 'pages.components.logs.page.description',
    defaultMessage: 'Show the logs of component'
  });

  useEffect(() => {
    fetchComponentInfo(componentName).then(function (component) {
      setAgentUrl(parseAgentUrl(component));
    });
  }, []);

  return <>
    <RuntimeLayout
      tabIndex={2} // @ts-ignore
      componentName={componentName}
      description={description + " (" + componentName + ")"}
      content={<LogTable componentName={componentName} agentUrl={agentUrl}/>}
    />
  </>
}


export default LogsPage;
