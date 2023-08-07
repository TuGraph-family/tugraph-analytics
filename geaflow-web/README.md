# Geaflow Opensource

## Quick Start

### 开发环境
```bash 
yarn install
yarn run dev
```

### 生产环境
```bash 
yarn install
yarn build
yarn start
```

### 自定义接口地址
默认接口地址为用户访问的 IP 或域名，端口默认为 8080：
```
`${window.location.protocol}//${window.location.hostname}:8080`;
```

当需要自定义接口地址时候，在启动服务之前，即执行 yarn start 之前，修改 app/client/dist/index.html 文件中的 window.GEAFLOW_HTTP_SERVICE_URL 值即可。
