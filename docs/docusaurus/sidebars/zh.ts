/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */

// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars_zh = {
  // By default, Docusaurus generates a sidebar from the docs folder structure
  tutorialSidebar: [
    "guide",
    "introduction",
    {
      type: "category",
      label: "快速开始",
      items: [
        "quick_start/quick_start",
        "quick_start/quick_start_docker",
        "quick_start/quick_start_infer&UDF",
        "quick_start/quick_start_sql_to_graph",
         ],
    },
    {
      type: "category",
      label: "技术原理",
      items: [
        "concepts/glossary",
        "concepts/dsl_principle",
        "concepts/framework_principle",
        "concepts/state_principle",
        "concepts/console_principle",
      ],
    },
    {
      type: "category",
      label: "开发指南",
      items: [
        {
          type: "category",
          label: "API开发",
          items: [
            "application-development/api/overview",
            {
              type: "category",
              label: "流API",
              items: [
                "application-development/api/stream/source",
                "application-development/api/stream/process",
                "application-development/api/stream/sink",
              ],
            },
            {
              type: "category",
              label: "图API",
              items: [
                "application-development/api/graph/traversal",
                "application-development/api/graph/compute",
              ],
            },
          ],
        },
        {
          type: "category",
          label: "DSL开发",
          items: [
            "application-development/dsl/overview",
            {
              type: "category",
              label: "语法文档",
              items: [
                "application-development/dsl/syntax/dcl",
                "application-development/dsl/syntax/ddl",
                "application-development/dsl/syntax/dml",
                {
                  type: "category",
                  label: "DQL",
                  items: [
                    "application-development/dsl/syntax/dql/match",
                    "application-development/dsl/syntax/dql/select",
                    "application-development/dsl/syntax/dql/union",
                    "application-development/dsl/syntax/dql/with",
                  ],
                },
              ],
            },
            {
              type: "category",
              label: "内置函数",
              items: [
                "application-development/dsl/build-in/aggregate",
                "application-development/dsl/build-in/condition",
                "application-development/dsl/build-in/date",
                "application-development/dsl/build-in/logical",
                "application-development/dsl/build-in/math",
                "application-development/dsl/build-in/string",
                "application-development/dsl/build-in/table",
              ],
            },
            {
              type: "category",
              label: "自定义函数",
              items: [
                "application-development/dsl/udf/udf",
                "application-development/dsl/udf/udaf",
                "application-development/dsl/udf/udtf",
                "application-development/dsl/udf/udga",
              ],
            },
          ],
        },
        {
          type: "category",
          label: "连接器(Connector)",
          items: [
            "application-development/connector/common",
            "application-development/connector/file",
            "application-development/connector/console",
            "application-development/connector/jdbc",
            "application-development/connector/hive",
            "application-development/connector/kafka",
            "application-development/connector/hbase",
            "application-development/connector/hudi",
            "application-development/connector/pulsar",
            "application-development/connector/udc",
          ],
        },
        "application-development/chat_guide",
      ],
    },
    {
      type: "category",
      label: "部署",
      items: [
        "deploy/install_guide",
        "deploy/quick_start_operator",
        "deploy/dashboard",
        {
          type: "doc",
          label: "🌈 G6VP 图可视化",
          id: "deploy/collaborate_with_g6vp",
        },
        "deploy/install_llm",
        "deploy/install_minikube",
      ],
    },
    "contribution",
    "contacts",
    "thanks",
    {
      type: "category",
      label: "参考资料",
      items: ["reference/vs_join"],
    },
  ],

  // But you can create a sidebar manually
  /*
    tutorialSidebar: [
      'intro',
      'hello',
      {
        type: 'category',
        label: 'Tutorial',
        items: ['tutorial-basics/create-a-document'],
      },
    ],
     */
};

module.exports = sidebars_zh;
