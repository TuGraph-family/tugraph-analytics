"use strict";(self.webpackChunkdocusaurus=self.webpackChunkdocusaurus||[]).push([[8065],{4568:(e,n,r)=>{r.d(n,{A:()=>l});const l=r.p+"assets/images/port_forward_operator-41593c5c5299bfda063818a90fbd75a2.png"},5121:(e,n,r)=>{r.r(n),r.d(n,{assets:()=>i,contentTitle:()=>o,default:()=>h,frontMatter:()=>s,metadata:()=>t,toc:()=>c});var l=r(4848),a=r(8453);const s={},o="K8S Operator\u90e8\u7f72",t={id:"deploy/quick_start_operator",title:"K8S Operator\u90e8\u7f72",description:"\u51c6\u5907\u5de5\u4f5c",source:"@site/../docs-cn/source/7.deploy/2.quick_start_operator.md",sourceDirName:"7.deploy",slug:"/deploy/quick_start_operator",permalink:"/tugraph-analytics/zh/deploy/quick_start_operator",draft:!1,unlisted:!1,tags:[],version:"current",sidebarPosition:2,frontMatter:{},sidebar:"tutorialSidebar",previous:{title:"K8S\u96c6\u7fa4\u90e8\u7f72",permalink:"/tugraph-analytics/zh/deploy/install_guide"},next:{title:"\u4f5c\u4e1aDashboard\u76d1\u63a7",permalink:"/tugraph-analytics/zh/deploy/dashboard"}},i={},c=[{value:"\u51c6\u5907\u5de5\u4f5c",id:"\u51c6\u5907\u5de5\u4f5c",level:2},{value:"\u5b89\u88c5Geaflow Kubernetes Operator",id:"\u5b89\u88c5geaflow-kubernetes-operator",level:2},{value:"\u901a\u8fc7Geaflow Kubernetes Operator\u63d0\u4ea4\u4f5c\u4e1a",id:"\u901a\u8fc7geaflow-kubernetes-operator\u63d0\u4ea4\u4f5c\u4e1a",level:2},{value:"\u63d0\u4ea4\u4f5c\u4e1a",id:"\u63d0\u4ea4\u4f5c\u4e1a",level:3},{value:"\u63d0\u4ea4HLA\u4f5c\u4e1a",id:"\u63d0\u4ea4hla\u4f5c\u4e1a",level:3},{value:"\u63d0\u4ea4DSL\u4f5c\u4e1a",id:"\u63d0\u4ea4dsl\u4f5c\u4e1a",level:3},{value:"\u67e5\u770b\u4f5c\u4e1a\u72b6\u6001",id:"\u67e5\u770b\u4f5c\u4e1a\u72b6\u6001",level:3},{value:"\u901a\u8fc7geaflow-kubernetes-operator-dashboard\u67e5\u770b",id:"\u901a\u8fc7geaflow-kubernetes-operator-dashboard\u67e5\u770b",level:4},{value:"\u901a\u8fc7\u547d\u4ee4\u884c\u67e5\u770b",id:"\u901a\u8fc7\u547d\u4ee4\u884c\u67e5\u770b",level:4}];function d(e){const n={a:"a",code:"code",h1:"h1",h2:"h2",h3:"h3",h4:"h4",header:"header",img:"img",li:"li",ol:"ol",p:"p",pre:"pre",strong:"strong",ul:"ul",...(0,a.R)(),...e.components};return(0,l.jsxs)(l.Fragment,{children:[(0,l.jsx)(n.header,{children:(0,l.jsx)(n.h1,{id:"k8s-operator\u90e8\u7f72",children:"K8S Operator\u90e8\u7f72"})}),"\n",(0,l.jsx)(n.h2,{id:"\u51c6\u5907\u5de5\u4f5c",children:"\u51c6\u5907\u5de5\u4f5c"}),"\n",(0,l.jsxs)(n.ol,{children:["\n",(0,l.jsxs)(n.li,{children:["\u4e0b\u8f7d\u5b89\u88c5docker\u548cminikube\u3002\u53c2\u8003\u6587\u6863\uff1a",(0,l.jsx)(n.a,{href:"/tugraph-analytics/zh/deploy/install_minikube",children:"\u5b89\u88c5minikube"})]}),"\n",(0,l.jsx)(n.li,{children:"\u62c9\u53d6GeaFlow\u955c\u50cf"}),"\n"]}),"\n",(0,l.jsx)(n.pre,{children:(0,l.jsx)(n.code,{className:"language-shell",children:"docker pull tugraph/geaflow:0.6\n"})}),"\n",(0,l.jsx)(n.p,{children:"\u5982\u679c\u9047\u5230\u7f51\u7edc\u95ee\u9898\u5bfc\u81f4\u62c9\u53d6\u5931\u8d25\uff0c\u4e5f\u53ef\u4ee5\u901a\u8fc7\u4e0b\u9762\u547d\u4ee4\u76f4\u63a5\u6784\u5efa\u955c\u50cf(\u6784\u5efa\u955c\u50cf\u4e4b\u524d\u9700\u8981\u5148\u542f\u52a8docker\u5bb9\u5668,\u6784\u5efa\u811a\u672c\u6839\u636e\u673a\u5668\u7c7b\u578bbuild\u5bf9\u5e94\u7c7b\u578b\u7684\u955c\u50cf):"}),"\n",(0,l.jsx)(n.pre,{children:(0,l.jsx)(n.code,{className:"language-shell",children:"git clone https://github.com/TuGraph-family/tugraph-analytics.git geaflow\ncd geaflow/\nbash ./build.sh --module=geaflow\n"})}),"\n",(0,l.jsx)(n.p,{children:"\u6574\u4e2a\u7f16\u8bd1\u8fc7\u7a0b\u53ef\u80fd\u6301\u7eed\u4e00\u6bb5\u65f6\u95f4\uff0c\u8bf7\u8010\u5fc3\u7b49\u5f85\u3002\u955c\u50cf\u7f16\u8bd1\u6210\u529f\u540e\uff0c\u901a\u8fc7\u4ee5\u4e0b\u547d\u4ee4\u67e5\u770b\u955c\u50cf\uff1a"}),"\n",(0,l.jsx)(n.pre,{children:(0,l.jsx)(n.code,{className:"language-shell",children:"docker images\n"})}),"\n",(0,l.jsxs)(n.p,{children:["\u8fdc\u7a0b\u62c9\u53d6\u7684\u955c\u50cf\u540d\u79f0\u4e3a\uff1a",(0,l.jsx)(n.strong,{children:"tugraph/geaflow:0.6"}),"\u3002\n\u672c\u5730\u955c\u50cf\u540d\u79f0\u4e3a\uff1a",(0,l.jsx)(n.strong,{children:"geaflow:0.1"}),"\uff0c\u53ea\u9700\u9009\u62e9\u4e00\u79cd\u65b9\u5f0f\u6784\u5efa\u955c\u50cf\u5373\u53ef\u3002"]}),"\n",(0,l.jsx)(n.h2,{id:"\u5b89\u88c5geaflow-kubernetes-operator",children:"\u5b89\u88c5Geaflow Kubernetes Operator"}),"\n",(0,l.jsx)(n.p,{children:"\u4e0b\u9762\u4ecb\u7ecd\u5982\u4f55\u5b89\u88c5Geaflow Kubernetes Operator\u3002"}),"\n",(0,l.jsxs)(n.ol,{children:["\n",(0,l.jsx)(n.li,{children:"\u4e0b\u8f7dGeaFlow\u4ee3\u7801\u3002"}),"\n"]}),"\n",(0,l.jsx)(n.pre,{children:(0,l.jsx)(n.code,{className:"language-shell",children:"git clone https://github.com/TuGraph-family/tugraph-analytics.git geaflow\ncd geaflow/\n"})}),"\n",(0,l.jsxs)(n.ol,{start:"2",children:["\n",(0,l.jsx)(n.li,{children:"\u6784\u5efaGeaflow Kubernetes Operator\u7684\u955c\u50cf"}),"\n"]}),"\n",(0,l.jsx)(n.pre,{children:(0,l.jsx)(n.code,{className:"language-shell",children:"docker pull tugraph/geaflow-kubernetes-operator:0.6\n"})}),"\n",(0,l.jsx)(n.p,{children:"\u5982\u679c\u9047\u5230\u7f51\u7edc\u95ee\u9898\u5bfc\u81f4\u62c9\u53d6\u5931\u8d25\uff0c\u4e5f\u53ef\u4ee5\u901a\u8fc7\u4e0b\u9762\u547d\u4ee4\u76f4\u63a5\u6784\u5efa\u955c\u50cf:"}),"\n",(0,l.jsx)(n.pre,{children:(0,l.jsx)(n.code,{className:"language-shell",children:"cd geaflow/geaflow-kubernetes-operator/\nbash ./build-operator.sh\n"})}),"\n",(0,l.jsx)(n.p,{children:"operator\u9879\u76ee\u6700\u4f4e\u8981\u6c42jdk11\u3002\u6574\u4e2a\u7f16\u8bd1\u8fc7\u7a0b\u53ef\u80fd\u6301\u7eed\u4e00\u6bb5\u65f6\u95f4\uff0c\u8bf7\u8010\u5fc3\u7b49\u5f85\u3002\u955c\u50cf\u7f16\u8bd1\u6210\u529f\u540e\uff0c\u901a\u8fc7\u4ee5\u4e0b\u547d\u4ee4\u67e5\u770b\u955c\u50cf\uff1a"}),"\n",(0,l.jsx)(n.pre,{children:(0,l.jsx)(n.code,{className:"language-shell",children:"docker images\n"})}),"\n",(0,l.jsxs)(n.p,{children:["\u8fdc\u7a0b\u62c9\u53d6\u7684\u955c\u50cf\u540d\u79f0\u4e3a\uff1a",(0,l.jsx)(n.strong,{children:"tugraph/geaflow-kubernetes-operator:0.6"}),"\u3002\n\u672c\u5730\u955c\u50cf\u540d\u79f0\u4e3a\uff1a",(0,l.jsx)(n.strong,{children:"geaflow-kubernetes-operator:0.1"}),"\uff0c\u53ea\u9700\u9009\u62e9\u4e00\u79cd\u65b9\u5f0f\u6784\u5efa\u955c\u50cf\u5373\u53ef\u3002"]}),"\n",(0,l.jsxs)(n.ol,{start:"3",children:["\n",(0,l.jsx)(n.li,{children:"\u786e\u8ba4helm\u7684\u955c\u50cf\u540d\u79f0"}),"\n"]}),"\n",(0,l.jsx)(n.p,{children:"\u6253\u5f00/helm/geaflow-kubernetes-operator/values.yaml\u3002"}),"\n",(0,l.jsx)(n.p,{children:"\u5982\u6709\u5fc5\u8981\u5219\u53ef\u4fee\u6539image.repository\u548cimage.tag\u4ee5\u4f7f\u7528\u6b63\u786e\u7684\u955c\u50cf\u540d\u3002"}),"\n",(0,l.jsxs)(n.ol,{start:"4",children:["\n",(0,l.jsx)(n.li,{children:"\u901a\u8fc7helm\u5b89\u88c5Geaflow Kubernetes Operator"}),"\n"]}),"\n",(0,l.jsx)(n.pre,{children:(0,l.jsx)(n.code,{className:"language-shell",children:"cd geaflow/geaflow-kubernetes-operator/\nhelm install geaflow-kubernetes-operator helm/geaflow-kubernetes-operator\n"})}),"\n",(0,l.jsx)(n.p,{children:(0,l.jsx)(n.img,{alt:"img.png",src:r(9529).A+"",width:"1500",height:"269"})}),"\n",(0,l.jsxs)(n.ol,{start:"5",children:["\n",(0,l.jsxs)(n.li,{children:["\u5728minikube dashboard\u4e2d\u67e5\u770bpod\u662f\u5426\u6b63\u5e38\u8fd0\u884c\n",(0,l.jsx)(n.img,{alt:"img.png",src:r(6748).A+"",width:"1500",height:"758"})]}),"\n",(0,l.jsx)(n.li,{children:"\u5c06GeaFlow-Operator-Dashboard\u901a\u8fc7portforward\u4ee3\u7406\u5230\u672c\u5730\u7aef\u53e3\uff08\u9ed8\u8ba4\u4e3a8089\u7aef\u53e3\uff09"}),"\n"]}),"\n",(0,l.jsx)(n.p,{children:"\u8bf7\u5c06operator-pod-name\u66ff\u6362\u4e3a\u5b9e\u9645\u7684operator pod\u540d\u79f0\u3002"}),"\n",(0,l.jsx)(n.pre,{children:(0,l.jsx)(n.code,{className:"language-shell",children:"kubectl port-forward ${operator-pod-name} 8089:8089\n"})}),"\n",(0,l.jsx)(n.p,{children:(0,l.jsx)(n.img,{alt:"img.png",src:r(4568).A+"",width:"1290",height:"358"})}),"\n",(0,l.jsxs)(n.ol,{start:"7",children:["\n",(0,l.jsxs)(n.li,{children:["\u6d4f\u89c8\u5668\u8bbf\u95eelocalhost:8089\u5373\u53ef\u6253\u5f00operator\u96c6\u7fa4\u9875\u9762\n",(0,l.jsx)(n.img,{alt:"img.png",src:r(5466).A+"",width:"1500",height:"818"})]}),"\n"]}),"\n",(0,l.jsx)(n.h2,{id:"\u901a\u8fc7geaflow-kubernetes-operator\u63d0\u4ea4\u4f5c\u4e1a",children:"\u901a\u8fc7Geaflow Kubernetes Operator\u63d0\u4ea4\u4f5c\u4e1a"}),"\n",(0,l.jsx)(n.p,{children:"Geaflow-kubernetes-operator\u6210\u529f\u90e8\u7f72\u5e76\u8fd0\u884c\u540e\uff0c\u5c31\u53ef\u4ee5\u7f16\u5199CR\u7684yaml\u6587\u4ef6\u8fdb\u884c\u4f5c\u4e1a\u63d0\u4ea4\u5566\u3002"}),"\n",(0,l.jsx)(n.h3,{id:"\u63d0\u4ea4\u4f5c\u4e1a",children:"\u63d0\u4ea4\u4f5c\u4e1a"}),"\n",(0,l.jsx)(n.p,{children:"geaflow-kubernetes-operator\u6210\u529f\u90e8\u7f72\u5e76\u8fd0\u884c\u540e\uff0c\u5c31\u53ef\u4ee5\u7f16\u5199\u4f5c\u4e1a\u7684yaml\u6587\u4ef6\u8fdb\u884c\u4f5c\u4e1a\u63d0\u4ea4\u4e86\u3002\n\u9996\u5148\u6211\u4eec\u7f16\u5199\u4e00\u4e2ageaflow\u5185\u7f6e\u793a\u4f8b\u4f5c\u4e1a\u7684yaml\u6587\u4ef6\u3002"}),"\n",(0,l.jsx)(n.pre,{children:(0,l.jsx)(n.code,{className:"language-yaml",children:'apiVersion: geaflow.antgroup.com/v1\nkind: GeaflowJob\nmetadata:\n  # \u4f5c\u4e1a\u540d\u79f0\n  name: geaflow-example\nspec:\n  # \u4f5c\u4e1a\u4f7f\u7528\u7684GeaFlow\u955c\u50cf\n  image: geaflow:0.1\n  # \u4f5c\u4e1a\u62c9\u53d6\u955c\u50cf\u7684\u7b56\u7565\n  imagePullPolicy: IfNotPresent\n  # \u4f5c\u4e1a\u4f7f\u7528\u7684k8s service account\n  serviceAccount: geaflow\n  # \u4f5c\u4e1ajava\u8fdb\u7a0b\u7684\u4e3b\u7c7b\n  entryClass: com.antgroup.geaflow.example.graph.statical.compute.khop.KHop\n  clientSpec:\n    # client pod\u76f8\u5173\u7684\u8d44\u6e90\u8bbe\u7f6e\n    resource:\n      cpuCores: 1\n      memoryMb: 1000\n      jvmOptions: -Xmx800m,-Xms800m,-Xmn300m\n  masterSpec:\n    # master pod\u76f8\u5173\u7684\u8d44\u6e90\u8bbe\u7f6e\n    resource:\n      cpuCores: 1\n      memoryMb: 1000\n      jvmOptions: -Xmx800m,-Xms800m,-Xmn300m\n  driverSpec:\n    # driver pod\u76f8\u5173\u7684\u8d44\u6e90\u8bbe\u7f6e\n    resource:\n      cpuCores: 1\n      memoryMb: 1000\n      jvmOptions: -Xmx800m,-Xms800m,-Xmn300m\n    # driver\u4e2a\u6570\n    driverNum: 1\n  containerSpec:\n    # container pod\u76f8\u5173\u7684\u8d44\u6e90\u8bbe\u7f6e\n    resource:\n      cpuCores: 1\n      memoryMb: 1000\n      jvmOptions: -Xmx800m,-Xms800m,-Xmn300m\n    # container\u4e2a\u6570\n    containerNum: 1\n    # \u6bcf\u4e2acontainer\u5185\u90e8\u7684worker\u4e2a\u6570(\u7ebf\u7a0b\u6570)\n    workerNumPerContainer: 4\n  userSpec:\n    # \u4f5c\u4e1a\u6307\u6807\u76f8\u5173\u914d\u7f6e\n    metricConfig:\n      geaflow.metric.reporters: slf4j\n      geaflow.metric.stats.type: memory\n    # \u4f5c\u4e1a\u5b58\u50a8\u76f8\u5173\u914d\u7f6e\n    stateConfig:\n      geaflow.file.persistent.type: LOCAL\n      geaflow.store.redis.host: host.minikube.internal\n      geaflow.store.redis.port: "6379"\n    # \u7528\u6237\u81ea\u5b9a\u4e49\u53c2\u6570\u914d\u7f6e\n    additionalArgs:\n      kubernetes.resource.storage.limit.size: 12Gi\n      geaflow.system.state.backend.type: MEMORY\n'})}),"\n",(0,l.jsx)(n.p,{children:"Geaflow \u4f5c\u4e1a\u4f9d\u8d56\u4e8eredis\u7ec4\u4ef6\uff0c\u53ef\u4ee5\u901a\u8fc7docker\u5feb\u901f\u542f\u52a8\u4e00\u4e2aredis\u5bb9\u5668\u5e76\u5c06\u7aef\u53e3\u6620\u5c04\u5230localhost\u3002"}),"\n",(0,l.jsx)(n.pre,{children:(0,l.jsx)(n.code,{className:"language-shell",children:"docker pull redis:latest\ndocker run -p 6379:6379 --name geaflow_redis redis:latest\n"})}),"\n",(0,l.jsx)(n.p,{children:"\u82e5\u4f60\u5df2\u7ecf\u90e8\u7f72\u4e86\u4e00\u4e2aredis\u7ec4\u4ef6\uff0c\u5219\u53ef\u4ee5\u5c06example.yaml\u4e2d\u7684\u4ee5\u4e0b\u53c2\u6570\u66ff\u6362\u4e3a\u5df2\u6709\u7684redis host\u548c\u7aef\u53e3\u53f7\u3002"}),"\n",(0,l.jsx)(n.pre,{children:(0,l.jsx)(n.code,{className:"language-yaml",children:"spec:\n  userSpec:\n    stateConfig:\n      geaflow.store.redis.host: ${your.redis.host}\n      geaflow.store.redis.port: ${your.redis.port}\n"})}),"\n",(0,l.jsx)(n.p,{children:"\u7136\u540e\u901a\u8fc7\u5982\u4e0b\u547d\u4ee4\u5373\u53ef\u5c06\u4f5c\u4e1a\u63d0\u4ea4\u5230k8s\u96c6\u7fa4\u3002"}),"\n",(0,l.jsx)(n.pre,{children:(0,l.jsx)(n.code,{className:"language-shell",children:"cd geaflow/geaflow-kubernetes-operator/example\nkubectl apply example_hla.yml\n"})}),"\n",(0,l.jsx)(n.h3,{id:"\u63d0\u4ea4hla\u4f5c\u4e1a",children:"\u63d0\u4ea4HLA\u4f5c\u4e1a"}),"\n",(0,l.jsx)(n.p,{children:"\u5bf9\u4e8e\u63d0\u4ea4HLA\u4f5c\u4e1a\u7684\u60c5\u51b5\uff0c\u9700\u8981\u989d\u5916\u6ce8\u610f\u4ee5\u4e0b\u51e0\u4e2a\u53c2\u6570\uff1a"}),"\n",(0,l.jsxs)(n.ul,{children:["\n",(0,l.jsx)(n.li,{children:"entryClass\u5fc5\u586b\u3002"}),"\n",(0,l.jsx)(n.li,{children:"udfJars\u9009\u586b\uff0c\u5982\u6709\u7684\u8bdd\u8bf7\u586b\u5199\u81ea\u5df1\u6587\u4ef6\u7684url\u5730\u5740\u3002"}),"\n"]}),"\n",(0,l.jsx)(n.pre,{children:(0,l.jsx)(n.code,{className:"language-yaml",children:"spec:\n  # \u5fc5\u586b\n  entryClass: com.example.MyEntryClass\n  # \u53ef\u9009\n  udfJars:\n    - name: myUdf.jar\n      url: http://localhost:8888/download/myUdf.jar\n"})}),"\n",(0,l.jsx)(n.h3,{id:"\u63d0\u4ea4dsl\u4f5c\u4e1a",children:"\u63d0\u4ea4DSL\u4f5c\u4e1a"}),"\n",(0,l.jsx)(n.p,{children:"\u5bf9\u4e8e\u63d0\u4ea4DSL\u4f5c\u4e1a\u7684\u60c5\u51b5\uff0c\u9700\u8981\u989d\u5916\u6ce8\u610f\u4ee5\u4e0b\u51e0\u4e2a\u53c2\u6570\uff1a"}),"\n",(0,l.jsxs)(n.ul,{children:["\n",(0,l.jsx)(n.li,{children:"entryClass\u4e0d\u586b\uff0c\u7559\u7a7a\u3002"}),"\n",(0,l.jsx)(n.li,{children:"gqlFile\u5fc5\u586b\uff0c\u8bf7\u586b\u5199\u81ea\u5df1\u6587\u4ef6\u7684\u540d\u79f0\u548curl\u5730\u5740\u3002"}),"\n",(0,l.jsx)(n.li,{children:"udfJars\u9009\u586b\uff0c\u5982\u6709\u7684\u8bdd\u8bf7\u586b\u5199\u81ea\u5df1\u6587\u4ef6\u7684url\u5730\u5740\u3002"}),"\n"]}),"\n",(0,l.jsx)(n.pre,{children:(0,l.jsx)(n.code,{className:"language-yaml",children:"spec:\n  # \u4e0d\u586b\n  # entryClass: com.example.MyEntryClass\n  # \u5fc5\u586b\n  gqlFile:\n    # name\u5fc5\u987b\u586b\u5199\u6b63\u786e\uff0c\u5426\u5219\u65e0\u6cd5\u627e\u5230\u5bf9\u5e94\u6587\u4ef6\n    name: myGql.gql\n    url: http://localhost:8888/download/myGql.gql\n  # \u53ef\u9009\n  udfJars:\n    - name: myUdf.jar\n      url: http://localhost:8888/download/myUdf.jar\n"})}),"\n",(0,l.jsx)(n.p,{children:"\u5173\u4e8eDSL\u4f5c\u4e1a\u548cHLA\u4f5c\u4e1a\u7684\u66f4\u591a\u53c2\u6570\uff0c\u6211\u4eec\u5728\u9879\u76ee\u76ee\u5f55geaflow-kubernetes-operator/example\u76ee\u5f55\u4e2d\u51c6\u5907\u4e86\u4e24\u4e2ademo\u4f5c\u4e1a\u4f9b\u5927\u5bb6\u53c2\u8003\uff0c\u8bf7\u5206\u522b\u53c2\u8003\u9879\u76ee\u4e2d\u7684\u793a\u4f8b\u6587\u4ef6\uff1a"}),"\n",(0,l.jsxs)(n.ul,{children:["\n",(0,l.jsx)(n.li,{children:"example/example-dsl.yml"}),"\n",(0,l.jsx)(n.li,{children:"example/example-hla.yml\u3002"}),"\n"]}),"\n",(0,l.jsx)(n.h3,{id:"\u67e5\u770b\u4f5c\u4e1a\u72b6\u6001",children:"\u67e5\u770b\u4f5c\u4e1a\u72b6\u6001"}),"\n",(0,l.jsx)(n.h4,{id:"\u901a\u8fc7geaflow-kubernetes-operator-dashboard\u67e5\u770b",children:"\u901a\u8fc7geaflow-kubernetes-operator-dashboard\u67e5\u770b"}),"\n",(0,l.jsxs)(n.p,{children:["\u6d4f\u89c8\u5668\u8bbf\u95ee",(0,l.jsx)(n.a,{href:"http://localhost:8089/%EF%BC%8C%E5%8D%B3%E5%8F%AF%E6%89%93%E5%BC%80%E9%9B%86%E7%BE%A4%E9%A1%B5%E9%9D%A2%E6%9F%A5%E7%9C%8B%E9%9B%86%E7%BE%A4%E4%B8%8B%E6%89%80%E6%9C%89geaflowjob%E4%BD%9C%E4%B8%9A%E5%88%97%E8%A1%A8%E5%92%8C%E8%AF%A6%E6%83%85%E3%80%82",children:"http://localhost:8089/\uff0c\u5373\u53ef\u6253\u5f00\u96c6\u7fa4\u9875\u9762\u67e5\u770b\u96c6\u7fa4\u4e0b\u6240\u6709geaflowjob\u4f5c\u4e1a\u5217\u8868\u548c\u8be6\u60c5\u3002"})]}),"\n",(0,l.jsx)(n.p,{children:(0,l.jsx)(n.img,{alt:"img.png",src:r(8263).A+"",width:"1500",height:"783"})}),"\n",(0,l.jsx)(n.h4,{id:"\u901a\u8fc7\u547d\u4ee4\u884c\u67e5\u770b",children:"\u901a\u8fc7\u547d\u4ee4\u884c\u67e5\u770b"}),"\n",(0,l.jsx)(n.p,{children:"\u6267\u884c\u4ee5\u4e0b\u547d\u4ee4\u53ef\u4ee5\u67e5\u770bCR\u7684\u72b6\u6001"}),"\n",(0,l.jsx)(n.pre,{children:(0,l.jsx)(n.code,{className:"language-shell",children:"kubectl get geaflowjob geaflow-example\n"})})]})}function h(e={}){const{wrapper:n}={...(0,a.R)(),...e.components};return n?(0,l.jsx)(n,{...e,children:(0,l.jsx)(d,{...e})}):d(e)}},5466:(e,n,r)=>{r.d(n,{A:()=>l});const l=r.p+"assets/images/operator_dashboard-cc184c6fbebb34889e5fa44b4acc9219.png"},6748:(e,n,r)=>{r.d(n,{A:()=>l});const l=r.p+"assets/images/view_operator_pod-3b486c663d770e0f12c491d7676164c7.png"},8263:(e,n,r)=>{r.d(n,{A:()=>l});const l=r.p+"assets/images/operator_dashboard_jobs-8a9aa816066c3e5942f0619ff91876de.png"},8453:(e,n,r)=>{r.d(n,{R:()=>o,x:()=>t});var l=r(6540);const a={},s=l.createContext(a);function o(e){const n=l.useContext(s);return l.useMemo((function(){return"function"==typeof e?e(n):{...n,...e}}),[n,e])}function t(e){let n;return n=e.disableParentContext?"function"==typeof e.components?e.components(a):e.components||a:o(e.components),l.createElement(s.Provider,{value:n},e.children)}},9529:(e,n,r)=>{r.d(n,{A:()=>l});const l=r.p+"assets/images/helm_install_operator-afd901fafc46cbcc9a043e3baf60a40c.png"}}]);