import React, {useEffect, useRef, useState} from 'react';
import {getLogContent} from "@/services/jobs/api";
import RuntimeLayout from "@/pages/Component/Runtime/Runtime";
import {history, useIntl, useParams} from "@@/exports";
import {fetchComponentInfo, getByteNum, parseAgentUrl} from "@/util/CommonUtil";
import {PaginationProps} from "antd";
import FileContentCard from "@/pages/Component/Runtime/FileContentCard";

const DEFAULT_PAGE_SIZE_KB = 50;

const LogContent: React.FC<{ path: string, componentName: string, logPath: string }> = ({
                                                                                          path,
                                                                                          componentName,
                                                                                          logPath
                                                                                        }) => {

  const logRequestRef = useRef<API.PageRequest>({pageNo: 1, pageSize: DEFAULT_PAGE_SIZE_KB});
  const [logInfo, setLogInfo]: [API.PageResponse<string>, any] = useState({total: 0} as API.PageResponse<string>);
  const agentUrlRef: React.MutableRefObject<string> = useRef("undefined");

  const intl = useIntl();

  const queryLog = async (agentUrl: string) => {
    let result = await getLogContent(agentUrl, logPath, logRequestRef.current.pageNo, getByteNum(logRequestRef.current.pageSize));
    setLogInfo(result.data);
  };

  const initAgentUrl = () => {
    fetchComponentInfo(componentName).then(function (componentInfo) {
      agentUrlRef.current = parseAgentUrl(componentInfo);
      queryLog(agentUrlRef.current).then();
    });
  }

  useEffect(initAgentUrl, []);

  const handlePaginationChange: PaginationProps['onChange'] = (page, pageSizeKB) => {
    logRequestRef.current = {pageNo: page, pageSize: pageSizeKB};
    queryLog(agentUrlRef.current);
  }

  return <FileContentCard
    title={intl.formatMessage({
      id: 'pages.components.log-content.card.description',
      defaultMessage: 'Log Content'
    })}
    subtitle={path}
    data={logInfo.data}
    totalBytes={logInfo.total}
    paginationRequestRef={logRequestRef}
    extra={null}
    handlePaginationChange={handlePaginationChange}/>
}

const LogInfoPage: React.FC = () => {

  const pathParams = useParams();
  const componentName = pathParams.componentName;
  const logPath = pathParams.logPath;

  if (componentName == null || componentName == 'undefined' || logPath == null || logPath == 'undefined') {
    history.push("/error");
    return null;
  }

  const intl = useIntl();

  const description = intl.formatMessage({
    id: 'pages.components.log-content.page.description',
    defaultMessage: 'Show the log content of component'
  });

  return <>
    <RuntimeLayout
      tabIndex={2} // @ts-ignore
      componentName={componentName}
      description={description + " (" + componentName + ")"}
      content={<LogContent path={logPath} componentName={componentName} logPath={logPath}/>}
    />
  </>
}


export default LogInfoPage;
