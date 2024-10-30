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
    "introduction",
    "quick_start",
    "quick_start_docker",
    {
      type: "category",
      label: "概念",
      items: [
        "concepts/glossary",
        "concepts/graph_view",
        "concepts/stream_graph",
      ],
    },
    {
      type: "category",
      label: "GeaFlow应用开发",
      items: [
        {
          type: "category",
          label: "API",
          items: [
            "application-development/api/overview",
            "application-development/api/guid",
            {
              type: "category",
              label: "图",
              items: [
                "application-development/api/graph/compute",
                "application-development/api/graph/traversal",
              ],
            },
            {
              type: "category",
              label: "流",
              items: [
                "application-development/api/stream/process",
                "application-development/api/stream/sink",
                "application-development/api/stream/source",
              ],
            },
          ],
        },
        {
          type: "category",
          label: "DSL",
          items: [
            "application-development/dsl/overview",
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
              label: "连接器(Connector)",
              items: [
                "application-development/dsl/connector/common",
                "application-development/dsl/connector/file",
                "application-development/dsl/connector/console",
                "application-development/dsl/connector/jdbc",
                "application-development/dsl/connector/hive",
                "application-development/dsl/connector/kafka",
                "application-development/dsl/connector/hbase",
                "application-development/dsl/connector/hudi",
                "application-development/dsl/connector/udc",
              ],
            },
            {
              type: "category",
              label: "语法文档",
              items: [
                "application-development/dsl/reference/ddl",
                "application-development/dsl/reference/dml",
                {
                  type: "category",
                  label: "DQL",
                  items: [
                    "application-development/dsl/reference/dql/match",
                    "application-development/dsl/reference/dql/select",
                    "application-development/dsl/reference/dql/union",
                    "application-development/dsl/reference/dql/with",
                  ],
                },
                "application-development/dsl/reference/use",
              ],
            },
            {
              type: "category",
              label: "UDF",
              items: [
                "application-development/dsl/udf/udaf",
                "application-development/dsl/udf/udf",
                "application-development/dsl/udf/udga",
                "application-development/dsl/udf/udtf",
              ],
            },
          ],
        },
      ],
    },
    {
      type: "category",
      label: "部署",
      items: ["deploy/install_guide"],
    },
    {
      type: "category",
      label: "原理",
      items: [
        "principle/dsl_principle",
        "principle/framework_principle",
        "principle/state_principle",
        "principle/vs_join",
      ],
    },
    "contribution",
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
