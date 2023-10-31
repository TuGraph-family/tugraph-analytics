export default {
  'pages.pagination.kb_per_page': 'KB / 页',
  'pages.pop-confirm.yes': '是',
  'pages.pop-confirm.no': '否',
  'pages.pipelineTable.page.title': 'Pipeline列表',
  'pages.pipelineTable.page.description': '展示作业所有的Pipeline开始时间、耗时等指标信息，点击Pipeline名称可以查看Cycle列表。',
  'pages.pipelineTable.name': '名称',
  'pages.pipelineTable.duration': '耗时',
  'pages.pipelineTable.startTime': '开始时间',
  'pages.cycleTable.page.title': 'CycleMetrics',
  'pages.cycleTable.page.description': '展示单个Pipeline的所有的Cycle的开始时间、耗时等指标信息。',
  'pages.cycleTable.name': '名称',
  'pages.cycleTable.opName': '算子名称',
  'pages.cycleTable.duration': '耗时',
  'pages.cycleTable.startTime': '开始时间',
  'pages.cycleTable.totalTasks': '总任务数',
  'pages.cycleTable.slowestTask': '最慢任务',
  'pages.cycleTable.slowestTaskExecuteTime': '最慢任务耗时',
  'pages.cycleTable.inputKb': '入流量',
  'pages.cycleTable.outputKb': '出流量',
  'pages.cycleTable.inputRecords': '入记录数',
  'pages.cycleTable.outputRecords': '出记录数',
  'pages.cycleTable.avgGcTime': '平均gc耗时',
  'pages.cycleTable.avgExecuteTime': '平均执行耗时',
  'pages.overview.page.title': 'Overview',
  'pages.overview.page.container.card.title': '集群健康状态',
  'pages.overview.page.description': 'Overview页面展示作业的主要指标，例如container的数量、运行中的worker数量等。',
  'pages.componentTable.id': 'id',
  'pages.componentTable.name': '名称',
  'pages.componentTable.host': 'Host',
  'pages.componentTable.pid': 'Pid',
  'pages.componentTable.lastTimestamp': '最新上报时间',
  'pages.componentTable.isActive': '心跳状态',
  'pages.componentTable.heapCommittedMB': '总堆内存',
  'pages.componentTable.heapUsedMB': '已使用堆内存',
  'pages.componentTable.heapUsedRatio': '堆内存使用率',
  'pages.componentTable.totalMemoryMB': '总内存',
  'pages.componentTable.gcCount': 'GC次数',
  'pages.componentTable.fgcCount': 'FGC次数',
  'pages.componentTable.availCores': '可用cpu核数',
  'pages.componentTable.usedCores': '已使用cpu核数',
  'pages.componentTable.activeThreads': '活跃线程数',
  'pages.componentTable.processCpu': 'cpu使用率',
  'pages.componentTable.componentState.active': '活跃',
  'pages.componentTable.componentState.timeout': '超时',
  'pages.driverTable.page.title': 'Driver列表',
  'pages.driverTable.page.description': '显示所有Driver的各项指标',
  'pages.containerTable.page.title': 'Container列表',
  'pages.containerTable.page.description': '显示所有Container的各项指标',
  'pages.componentConfiguration.key': '配置名称',
  'pages.componentConfiguration.value': '配置值',
  'pages.processMetrics.heapCommittedMB': '堆总内存',
  'pages.processMetrics.heapUsedMB': '堆已使用',
  'pages.processMetrics.heapUsedRatio': '堆内存使用率',
  'pages.processMetrics.totalMemoryMB': '总内存',
  'pages.processMetrics.fgcCount': 'fgc次数',
  'pages.processMetrics.fgcTime': 'fgc时间',
  'pages.processMetrics.gcTime': 'gc时间',
  'pages.processMetrics.gcCount': 'gc次数',
  'pages.processMetrics.avgLoad': 'avgLoad',
  'pages.processMetrics.availCores': '可用CPU核数',
  'pages.processMetrics.usedCores': '已使用CPU核数',
  'pages.processMetrics.processCpu': 'CPU使用率',
  'pages.processMetrics.activeThreads': '活跃线程数',
  'pages.components.metrics.page.title': '指标',
  'pages.components.metrics.page.description': '显示容器的进程指标',
  'pages.components.logs.page.title': '日志',
  'pages.components.logs.table.description': '日志列表',
  'pages.components.logs.table.path': '日志路径',
  'pages.components.logs.table.size': '大小',
  'pages.components.logs.page.description': '显示容器的日志列表',
  'pages.components.log-content.page.description': '显示容器日志内容',
  'pages.components.log-content.card.description': '日志内容',
  'pages.components.configuration.page.title': '配置',
  'pages.components.configuration.page.description': '显示master的配置项',
  'pages.components.configuration.table.title': 'Master进程配置一览',
  'pages.table.columns.search.placeholder': '输入以查询',
  'pages.table.columns.search.search-button': '搜索',
  'pages.table.columns.search.reset-button': '重置',
  'pages.components.flame-graphs.page.title': '火焰图',
  'pages.components.flame-graphs.page.description': '显示容器的火焰图执行历史',
  'pages.components.flame-graphs.table.description': '火焰图执行历史',
  'pages.components.flame-graphs.table.new': '新建',
  'pages.components.flame-graphs.table.path': '路径',
  'pages.components.flame-graphs.table.size': '大小',
  'pages.components.flame-graphs.table.createdTime': '创建时间',
  'pages.components.flame-graphs.table.operation': '操作',
  'pages.components.flame-graphs.table.operation.delete': '删除',
  'pages.components.flame-graphs.table.operation.delete.confirmMessage': '确定删除该文件？',
  'pages.components.flame-graphs.table.createForm.new': '新建火焰图',
  'pages.components.flame-graphs.table.createForm.profilerTypeLabel': '火焰图类型',
  'pages.components.flame-graphs.table.createForm.durationLabel': '执行时间 (秒)',
  'pages.components.flame-graphs.table.createForm.durationContent': '请输入执行时间. (1 ~ 60秒)',
  'pages.components.flame-graphs.table.createForm.loadingMessage': '执行中，请等候...',
  'pages.components.flame-graphs.table.createForm.successMessage': '已在后台开始执行，请等待输入的时间后，刷新列表查看',
  'pages.components.flame-graphs.table.createForm.errorMessage': '执行失败',
  'pages.components.flame-graphs.table.deleteForm.loadingMessage': '删除中...',
  'pages.components.flame-graphs.table.deleteForm.successMessage': '删除成功',
  'pages.components.flame-graphs.table.deleteForm.errorMessage': '删除失败',
  'pages.components.flame-graph-content.page.description': '显示容器的火焰图详情',
  'pages.components.flame-graph-content.card.description': '火焰图详情',
  'pages.components.thread-dump.page.title': 'Thread Dump',
  'pages.components.thread-dump.page.description': '显示Thread Dump结果',
  'pages.components.thread-dump.card.description': 'Thread Dump',
  'pages.components.thread-dump.card.lastDumpTime': '最新执行时间',
  'pages.components.thread-dump.card.emptyDumpTime': '尚未执行过',
  'pages.components.thread-dump.card.reload': '重新执行',
  'pages.components.thread-dump.card.regenerate.waiting': '正在Dump，请等待执行完成...',
  'pages.components.thread-dump.card.regenerate.success': 'Dump完成！结果已刷新。'
};
