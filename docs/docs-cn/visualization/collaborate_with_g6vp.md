# 🌈 [G6VP](https://github.com/antvis/g6vp) 现在支持与 Tugraph 协作实现流图作业可视化了！

## 仅需 5 步，即可呈现 🎊

### 1. 启动 GeaFlow 流图作业和 Socket 服务

参考 [快速开始](https://github.com/TuGraph-family/tugraph-analytics/blob/master/docs/docs-cn/quick_start.md)

⚠️ 注意在 `启动SocketServer` 步骤使用下列命令代替

```bash
bin/socket.sh 9003 GI
```

输出下列内容时，即表示 Tugraph Analytics 准备好建立连接

<img width="610" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/a25ed6ba-4fb9-4db1-9325-ee2f26a4337f">

> 如启动服务过程中遇到问题，可见 https://github.com/TuGraph-family/tugraph-analytics/issues/1

### 2. 创建 G6VP 项目

进入[新建画布](https://insight.antv.antgroup.com/#/workbook/create)，输入工作簿名称。我们会在后面手动添加点边数据，所以这里随便选择一个案例数据集即可，模版使用**极简模版**

### 3. 添加组件

我们需要添加两个组件，在工具栏中添加 **清空画布**；然后在默认布局的侧边容器中添加**环路检测 Demo**

<img width="463" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/b01271b5-162c-4216-9a9c-bf7a5570c999">
<img width="474" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/238685ec-d9cf-4fcf-8357-56f4f8a8928d">

> 此时项目画布应该如下所示
> <img width="1149" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/e660fa5b-aa31-4e7e-b295-cb071cc476c1">

点击工具栏中的`🧹清除`选项来清空画布节点

<img width="241" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/61316029-71ba-410f-94bf-47c6c65aec34">

默认情况下，添加完`环路检测Demo`组件后，会自动建立连接。

<img width="328" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/5246536b-ddb0-4c3c-91fb-e941101e272a">

Tugraph Analytics 端建立连接后同样会输出以下内容：

<img width="616" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/46be1e88-9c93-430e-92cc-db8024691095">

### 4. 演示

环路检测 Demo 提供了两种方式来进行交互：

* 方式一 在输入框中输入点边信息
* 方式二 使用内置数据进行演示

> 两种方式本质都是调用 Tugraph Analytics 进行实时计算，不过方式二省略了手动输入过程。

这里我们使用内置数据进行快速演示，点击【选项】，选择`添加点`，画布中出现了 7 个点信息；接着选择`添加边`。我们可以在上方对话框中看到添加记录。

<img width="332" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/7ca76607-41a1-4afe-9427-cf7599de6889">

同样的，Tugraph Analytics 终端也会实时输出操作信息，并自动启动计算任务。

<img width="611" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/d8d0d73a-4c07-4ecd-bcac-4633a742933a">

### 5. 结果展示

Tugraph Analytics 完成环路检测计算任务后，会自动返回检测结果。

<img width="324" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/ba343acf-812a-4df5-8da4-ff70e0b2531d">

右侧画布中会动态显示出本次环路检测结果信息：

![Jun-12-2023 19-53-35](https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/f8595322-d477-4702-a52e-4f03092b7219)