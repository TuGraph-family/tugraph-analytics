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
    sourceLang: localStorage.getItem('i18nextLng') || (navigator.language === ('en'|| 'en-US') ? 'en-US' : 'zh-CN')
  },
};
