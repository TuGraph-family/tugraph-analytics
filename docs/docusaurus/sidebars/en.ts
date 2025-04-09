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
const sidebars_en = {
  // By default, Docusaurus generates a sidebar from the docs folder structure
  tutorialSidebar: [
    "guide",
    "introduction",
    {
      type: "category",
      label: "Quick Start",
      items: [
        "quick_start/quick_start",
        "quick_start/quick_start_docker",
        "quick_start/quick_start_infer&UDF",
        "quick_start/quick_start_sql_to_graph",
      ],
    },
    {
      type: "category",
      label: "Concepts",
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
      label: "Development",
      items: [
        {
          type: "category",
          label: "API",
          items: [
            "application-development/api/overview",
            {
              type: "category",
              label: "Stream",
              items: [
                "application-development/api/stream/source",
                "application-development/api/stream/process",
                "application-development/api/stream/sink",
              ],
            },
            {
              type: "category",
              label: "Graph",
              items: [
                "application-development/api/graph/traversal",
                "application-development/api/graph/compute",
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
              label: "Syntax",
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
              label: "Build-In",
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
              label: "UDF",
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
          label: "Connector",
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
      label: "Deployment",
      items: [
        "deploy/install_guide",
        "deploy/quick_start_operator",
        "deploy/dashboard",
        {
          type: "doc",
          label: "🌈 G6VP Graph Visualization",
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
      label: "Reference",
      items: ["reference/vs_join"],
    },
  ],
};

module.exports = sidebars_en;
