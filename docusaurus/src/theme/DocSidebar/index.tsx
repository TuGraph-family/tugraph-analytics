import React, { useEffect, useMemo, useRef } from "react";
import DocSidebar from "@theme-original/DocSidebar";
import type DocSidebarType from "@theme/DocSidebar";
import type { WrapperProps } from "@docusaurus/types";
import { useLocation, useHistory } from "react-router-dom";
import { DocSearch } from "@docsearch/react";
import Link from "@docusaurus/Link";
import { EN_TRANSLATIONS, ZH_TRANSLATIONS } from "@site/src/constants";

type Props = WrapperProps<typeof DocSidebarType>;

export default function DocSidebarWrapper(props: Props): JSX.Element {
  const location = useLocation();
  const history = useHistory();
  const { pathname } = location;
  const languages = ["en", "zh"];

  const getCurrentLanguage = () => {
    const segments = pathname.split("/");
    return languages.find((lang) => segments.includes(lang)) || "en";
  };

  const Hit: React.FC = ({ hit, children }) => {
    console.log(hit, "hit");
    return <Link to={hit.url}>{children}</Link>;
  };

  useEffect(() => {
    window.addEventListener("click", () => {
      const currentPath = window.location.pathname;
      window.parent.postMessage({ path: currentPath }, "*");
    });
    window.addEventListener("hashchange", () => {
      const currentPath = window.location.pathname;
      const hash = window.location.hash;
      window.parent.postMessage({ path: currentPath + hash }, "*");
    });
  }, []);

  const getTranslationsByLanguage = (lang: string) => {
    if (lang === "zh") {
      return ZH_TRANSLATIONS;
    }
    return EN_TRANSLATIONS;
  };

  const getPlaceholderByLanguage = (lang: string) => {
    if (lang === "zh") {
      return "搜索文档";
    }
    return "Search docs";
  };

  const navigator = useRef({
    navigate({ itemUrl }: { itemUrl?: string }) {
      history.push(itemUrl!);
    },
  }).current;

  const indexName = useMemo(() => {
    const lang = getCurrentLanguage();

    return lang === "zh" ? "tugraphAnalyticsZH" : "tugraphAnalyticsEN";
  }, [location.pathname]);

  return (
    <div
      style={{
        display: "flex",
        justifyContent: "center",
        flexDirection: "column",
      }}
    >
      <div style={{ margin: "10px 4px 8px 8px" }}>
        <DocSearch
          {...{
            apiKey: "3c4b435fb8814030c3a6672abc015ff2",
            indexName,
            appId: "HO4M21RAQI",
            hitComponent: Hit,
            transformItems: (items) => {
              return items.map((item) => {
                return {
                  ...item,
                  url:
                    "/tugraph-analytics" +
                      item?.url?.split("/tugraph-analytics")[1] ?? "",
                };
              });
            },
            navigator: navigator,
            translations: getTranslationsByLanguage(getCurrentLanguage()),
            placeholder: getPlaceholderByLanguage(getCurrentLanguage()),
          }}
        />
      </div>
      <DocSidebar {...props} />
    </div>
  );
}
