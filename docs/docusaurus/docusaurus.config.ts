import { themes as prismThemes } from "prism-react-renderer";
import type { Config } from "@docusaurus/types";
import type * as Preset from "@docusaurus/preset-classic";

const config: Config = {
  title: "TuGraph Analytics",
  tagline: "Dinosaurs are cool",
  favicon:
    "https://mdn.alipayobjects.com/huamei_qcdryc/afts/img/A*AbamQ5lxv0IAAAAAAAAAAAAADgOBAQ/original",

  // Set the production url of your site here
  url: "https://liukaiming-alipay.github.io/",
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: "/tugraph-analytics",

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: "facebook", // Usually your GitHub org/user name.
  projectName: "docusaurus", // Usually your repo name.

  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "warn",

  markdown: {
    format: "md",
    mermaid: true,
    preprocessor: ({ filePath, fileContent }) => {
      if (filePath.includes("application-development/api/overview")) {
        const regex = /<([A-Z\s,]+)>/g;
        const newContent = fileContent
          ?.replace(regex, "")
          ?.replace(/<IVertex>|<IEdge>/g, "");
        return newContent;
      }
      // 使用正则表达式替换匹配到的标签为空字符串
      return fileContent;
    },
  },

  presets: [
    [
      "classic",
      {
        docs: {
          sidebarPath: "./sidebars/zh.ts",
          path: "./docs/docs-cn/source",
          routeBasePath: "zh",
        },
        theme: {
          customCss: "./src/css/custom.css",
        },
      },
    ],
  ],

  plugins: [
    [
      "content-docs",
      {
        id: "en",
        sidebarPath: "./sidebars/en.ts",
        path: "./docs/docs-en/source",
        routeBasePath: "en",
        editCurrentVersion: false,
      },
    ],
  ],

  themeConfig: {
    algolia: {
      apiKey: "3c4b435fb8814030c3a6672abc015ff2",
      indexName: "tugraphAnalyticsZH",
      appId: "HO4M21RAQI",
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
    },
  } satisfies Preset.ThemeConfig,

  headTags: [
    {
      tagName: "meta",
      attributes: {
        name: "algolia-site-verification",
        content: "4AB782AC2021573E",
      },
    },
  ],
};

export default config;
