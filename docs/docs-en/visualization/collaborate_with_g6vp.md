# 🌈 [G6VP](https://github.com/antvis/g6vp) now supports visualization of flow graph jobs in collaboration with Tugraph!

## Just 5 steps to present 🎊

### 1. Start the GeaFlow calculating job and Socket service

Reference [Quick Start](https://github.com/TuGraph-family/tugraph-analytics/blob/master/docs/docs-cn/quick_start.md)

⚠️ Note that in the 'start SocketServer' step use the following command instead

```bash
bin/socket.sh 9003 GI
```

When the terminal outputs the following, Tugraph Analytics is ready to establish a connection.

<img width="610" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/a25ed6ba-4fb9-4db1-9325-ee2f26a4337f">

> If any problem occurs during service startup, see https://github.com/TuGraph-family/tugraph-analytics/issues/1

### 2. Create a G6VP project

Enter [New Canvas](https://insight.antv.antgroup.com/#/workbook/create), enter a workbook name. We will manually add the dot edge data later, so choose a case data set here, and use the **minimalist template**

### 3. Add Components

We need to add two components, in the toolbar add **Clean canvas**; Then add **Loop Detection Demo** to the side container of the default layout

<img width="463" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/b01271b5-162c-4216-9a9c-bf7a5570c999">
<img width="474" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/238685ec-d9cf-4fcf-8357-56f4f8a8928d">

> The project canvas should look like this
> <img width="1149" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/e660fa5b-aa31-4e7e-b295-cb071cc476c1">

Click the '🧹 Clear' option in the toolbar to clear the canvas node

<img width="241" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/61316029-71ba-410f-94bf-47c6c65aec34">

By default, a connection is automatically established after the Loop Detection Demo component is added.

<img width="328" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/5246536b-ddb0-4c3c-91fb-e941101e272a">

Tugraph Analytics will also output the following after the connection is established:

<img width="616" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/46be1e88-9c93-430e-92cc-db8024691095">

### 4. Demostration

Loop detection Demo provides two ways to interact:

* Method 1 Enter the dot information in the input box
* Method 2 Demonstrate using built-in data

> Both methods essentially call Tugraph Analytics for real-time calculations, but Method 2 omits the manual input process.

Here we use the built-in data for a quick demonstration, click [Options], select 'Add Points', 7 points of information appear in the canvas; Then select 'Add Edges'. We can see the add record in the above dialog.

<img width="332" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/7ca76607-41a1-4afe-9427-cf7599de6889">

Similarly, the Tugraph Analytics terminal outputs operational information in real time and automatically starts computation tasks.

<img width="611" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/d8d0d73a-4c07-4ecd-bcac-4633a742933a">

### 5. Result Presentation

After the loop detection calculation task is completed, Tugraph Analytics automatically returns the detection results.

<img width="324" alt="image" src="https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/ba343acf-812a-4df5-8da4-ff70e0b2531d">

The loop detection results are dynamically displayed on the right canvas:

![Jun-12-2023 19-53-35](https://github.com/TuGraph-family/tugraph-analytics/assets/25787943/f8595322-d477-4702-a52e-4f03092b7219)