"use strict";(self.webpackChunkdocusaurus=self.webpackChunkdocusaurus||[]).push([[7769],{5609:(n,t,e)=>{e.r(t),e.d(t,{assets:()=>o,contentTitle:()=>r,default:()=>u,frontMatter:()=>l,metadata:()=>a,toc:()=>c});var s=e(4848),i=e(8453);const l={},r="UDTF\u4ecb\u7ecd",a={id:"application-development/dsl/udf/udtf",title:"UDTF\u4ecb\u7ecd",description:"UDTF\uff08User Defined Table Function\uff09\u5c06\u8f93\u5165\u6269\u5c55\u4e3a\u591a\u884c\u3002",source:"@site/../docs-cn/source/5.application-development/2.dsl/4.udf/3.udtf.md",sourceDirName:"5.application-development/2.dsl/4.udf",slug:"/application-development/dsl/udf/udtf",permalink:"/tugraph-analytics/zh/application-development/dsl/udf/udtf",draft:!1,unlisted:!1,tags:[],version:"current",sidebarPosition:3,frontMatter:{},sidebar:"tutorialSidebar",previous:{title:"UDAF\u4ecb\u7ecd",permalink:"/tugraph-analytics/zh/application-development/dsl/udf/udaf"},next:{title:"UDGA\u4ecb\u7ecd",permalink:"/tugraph-analytics/zh/application-development/dsl/udf/udga"}},o={},c=[{value:"\u63a5\u53e3",id:"\u63a5\u53e3",level:2},{value:"\u793a\u4f8b",id:"\u793a\u4f8b",level:2}];function d(n){const t={code:"code",h1:"h1",h2:"h2",header:"header",p:"p",pre:"pre",strong:"strong",...(0,i.R)(),...n.components};return(0,s.jsxs)(s.Fragment,{children:[(0,s.jsx)(t.header,{children:(0,s.jsx)(t.h1,{id:"udtf\u4ecb\u7ecd",children:"UDTF\u4ecb\u7ecd"})}),"\n",(0,s.jsx)(t.p,{children:"UDTF\uff08User Defined Table Function\uff09\u5c06\u8f93\u5165\u6269\u5c55\u4e3a\u591a\u884c\u3002"}),"\n",(0,s.jsx)(t.h2,{id:"\u63a5\u53e3",children:"\u63a5\u53e3"}),"\n",(0,s.jsx)(t.pre,{children:(0,s.jsx)(t.code,{className:"language-java",children:"public abstract class UserDefinedFunction implements Serializable {\n\n   /**\n     * Init method for the user defined function.\n     */\n    public void open(FunctionContext context) {\n    }\n\n    /**\n     * Close method for the user defined function.\n     */\n    public void close() {\n    }\n}\n\npublic abstract class UDTF extends UserDefinedFunction {\n\n    protected List<Object[]> collector;\n\n    public UDTF() {\n        this.collector = Lists.newArrayList();\n    }\n\t\n\t/**\n     * Collect the result.\n     */\n    protected void collect(Object[] output) {\n        \n    }\n\t\n\t/**\n     * Returns type output types for the function.\n     * @param paramTypes The parameter types of the function.\n     * @param outFieldNames The output fields of the function in the sql.\n     */\n    public abstract List<Class<?>> getReturnType(List<Class<?>> paramTypes, \n\t\t\t\t\t\t\t\t\t\t\t\t List<String> outFieldNames);\n}\n\n"})}),"\n",(0,s.jsxs)(t.p,{children:["\u6bcf\u4e2aUDTF\u90fd\u5e94\u8be5\u6709\u4e00\u4e2a\u6216\u591a\u4e2a",(0,s.jsx)(t.strong,{children:"eval"}),"\u65b9\u6cd5\u3002"]}),"\n",(0,s.jsx)(t.h2,{id:"\u793a\u4f8b",children:"\u793a\u4f8b"}),"\n",(0,s.jsx)(t.pre,{children:(0,s.jsx)(t.code,{className:"language-java",children:'public class Split extends UDTF {\n\n    private String splitChar = ",";\n\n    public void eval(String text) {\n        evalInternal(text);\n    }\n\n    public void eval(String text, String separator) {\n        evalInternal(text, separator);\n    }\n\n    private void evalInternal(String... args) {\n        if (args != null && (args.length == 1 || args.length == 2)) {\n            if (args.length == 2 && StringUtils.isNotEmpty(args[1])) {\n                splitChar = args[1];\n            }\n            String[] lines = StringUtils.split(args[0], splitChar);\n            for (String line : lines) {\n                collect(new Object[]{line});\n            }\n        }\n    }\n\n    @Override\n    public List<Class<?>> getReturnType(List<Class<?>> paramTypes, \n\t\t\t\t\t\t\t\t\t\tList<String> outputFields) {\n        return Collections.singletonList(String.class);\n    }\n}\n'})}),"\n",(0,s.jsx)(t.pre,{children:(0,s.jsx)(t.code,{className:"language-sql",children:"CREATE Function my_split AS 'com.antgroup.geaflow.dsl.udf.Split';\n\nSELECT t.id, u.name FROM users u, LATERAL table(my_split(u.ids)) as t(id);\n"})})]})}function u(n={}){const{wrapper:t}={...(0,i.R)(),...n.components};return t?(0,s.jsx)(t,{...n,children:(0,s.jsx)(d,{...n})}):d(n)}},8453:(n,t,e)=>{e.d(t,{R:()=>r,x:()=>a});var s=e(6540);const i={},l=s.createContext(i);function r(n){const t=s.useContext(l);return s.useMemo((function(){return"function"==typeof n?n(t):{...t,...n}}),[t,n])}function a(n){let t;return t=n.disableParentContext?"function"==typeof n.components?n.components(i):n.components||i:r(n.components),s.createElement(l.Provider,{value:t},n.children)}}}]);