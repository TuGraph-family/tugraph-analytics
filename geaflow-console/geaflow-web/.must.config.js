/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// more config: http://gitlab.alibaba-inc.com/parrot/parrot-tool-must/blob/master/doc/config.md
module.exports = {
  extract: {
    name: "openpiece-geaflow",
    sourcePath: "src/components",
    fileType: "ts",
    prettier: true,
    macro: {
      path: "src/components/i18n",
      method: '$i18n.get({id:"$key$",dm:"$defaultMessage$"})',
      import: 'import $i18n from "src/components/i18n"',
      keySplitter: ".",
      placeholder: (variable) => {
        return `{${variable}}`;
      },
      dependencies: ["@aligov/global-locale", "@aligov/global-string-format"],
    },
    babel: {
      allowImportExportEverywhere: true,
      decoratorsBeforeExport: true,
      plugins: [
        "asyncGenerators",
        "classProperties",
        "decorators-legacy",
        "doExpressions",
        "exportExtensions",
        "exportDefaultFrom",
        "typescript",
        "functionSent",
        "functionBind",
        "jsx",
        "objectRestSpread",
        "dynamicImport",
        "numericSeparator",
        "optionalChaining",
        "optionalCatchBinding",
      ],
    },
    isNeedUploadCopyToMedusa: true,
    sourceLang:
      localStorage.getItem("i18nextLng") ||
      (navigator.language === ("en" || "en-US") ? "en-US" : "zh-CN"),
  },
};
