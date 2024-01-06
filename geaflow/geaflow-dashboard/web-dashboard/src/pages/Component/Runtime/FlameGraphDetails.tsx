import React, {useEffect, useRef, useState} from 'react';
import {getFlameGraphContent} from "@/services/jobs/api";
import RuntimeLayout from "@/pages/Component/Runtime/Runtime";
import {history, useIntl, useParams} from "@@/exports";
import {fetchComponentInfo, parseAgentUrl} from "@/util/CommonUtil";

import {ProCard} from "@ant-design/pro-components";

const FlameGraphDisplay: React.FC<{path: string, content: string}> = ({path, content}) => {

  const intl = useIntl();

  const iframeRef = useRef(null);

  useEffect(() => {
    const iframe: any = iframeRef.current;
    const handleIframeLoad = () => {
      iframe.style.height = iframe.contentWindow.document.body.scrollHeight + 'px';
    };
    iframe.addEventListener('load', handleIframeLoad);
    return () => {
      iframe.removeEventListener('load', handleIframeLoad);
    };
  }, []);

  return <ProCard
    title={intl.formatMessage({
      id: 'pages.components.flame-graph-content.card.description',
      defaultMessage: 'Flame Graph Details'})}
    subTitle={path}
  >
    {/*<div dangerouslySetInnerHTML={{ __html: content }}></div>*/}
    <div>
      <iframe id="iframe"
              ref={iframeRef}
              name="flameGraphContent"
              srcDoc={content}
              frameBorder="0"
              scrolling="no"
              style={{ width: '100%', overflow: 'hidden' }}
      ></iframe>
    </div>
  </ProCard>
}

const LogInfoPage: React.FC = () => {
  const [flameGraphContent, setFlameGraphContent]: [string, any]  = useState("");

  const pathParams = useParams();
  const componentName = pathParams.componentName;
  const filePath = pathParams.filePath;

  if (componentName == null || componentName == 'undefined' || filePath == null || filePath == 'undefined') {
    history.push("/error");
    return null;
  }

  const intl = useIntl();

  const description = intl.formatMessage({
    id: 'pages.components.flame-graph-content.page.description',
    defaultMessage: 'Show the flame graph details of component'});

  useEffect(() => {
    const fetchFlameGraphContent = async (agentUrl: string, path: string) => {
      let result = await getFlameGraphContent(agentUrl, path);
      if (result.success) {
        setFlameGraphContent(result.data);
      } else {

      }
    };
    fetchComponentInfo(componentName).then(function (component) {
      fetchFlameGraphContent(parseAgentUrl(component), filePath).then();
    });
  }, []);

  return <>
    <RuntimeLayout
      tabIndex={3} // @ts-ignore
      componentName={componentName}
      description={description + " (" + componentName + ")"}
      content={<FlameGraphDisplay path={filePath} content={flameGraphContent} />}
    />
  </>
}


export default LogInfoPage;
