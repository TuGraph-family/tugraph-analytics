package com.antgroup.geaflow.runtime.core.scheduler.statemachine;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CLUSTER_ID;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_HEAP_SIZE_MB;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.JOB_UNIQUE_ID;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.RUN_LOCAL_MODE;
import static com.antgroup.geaflow.common.config.keys.FrameworkConfigKeys.SYSTEM_STATE_BACKEND_TYPE;

import com.antgroup.geaflow.cluster.protocol.ScheduleStateType;
import com.antgroup.geaflow.cluster.system.ClusterMetaStore;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.core.graph.CycleGroupType;
import com.antgroup.geaflow.core.graph.ExecutionVertex;
import com.antgroup.geaflow.core.graph.ExecutionVertexGroup;
import com.antgroup.geaflow.runtime.core.scheduler.BaseCycleSchedulerTest;
import com.antgroup.geaflow.runtime.core.scheduler.PipelineCycleScheduler;
import com.antgroup.geaflow.runtime.core.scheduler.context.CheckpointSchedulerContext;
import com.antgroup.geaflow.runtime.core.scheduler.context.CycleSchedulerContextFactory;
import com.antgroup.geaflow.runtime.core.scheduler.context.IterationRedoSchedulerContext;
import com.antgroup.geaflow.runtime.core.scheduler.context.RedoSchedulerContext;
import com.antgroup.geaflow.runtime.core.scheduler.cycle.ExecutionNodeCycle;
import com.antgroup.geaflow.runtime.core.scheduler.resource.AbstractScheduledWorkerManager;
import com.antgroup.geaflow.runtime.core.scheduler.statemachine.pipeline.PipelineStateMachine;
import com.antgroup.geaflow.shuffle.service.ShuffleManager;
import com.antgroup.geaflow.state.StoreType;
import com.antgroup.geaflow.stats.collector.StatsCollectorFactory;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StateMachineTest extends BaseCycleSchedulerTest {

    private Configuration configuration;

    @BeforeMethod
    public void setUp() {
        Map<String, String> config = new HashMap<>();
        config.put(JOB_UNIQUE_ID.getKey(),
            "scheduler-state-machine-test" + System.currentTimeMillis());
        config.put(RUN_LOCAL_MODE.getKey(), "true");
        config.put(SYSTEM_STATE_BACKEND_TYPE.getKey(), StoreType.MEMORY.name());
        config.put(CONTAINER_HEAP_SIZE_MB.getKey(), String.valueOf(1024));
        configuration = new Configuration(config);
        ClusterMetaStore.init(0, "driver-0", configuration);
        ShuffleManager.init(configuration);
        ShuffleManager.getInstance().initShuffleMaster();
        StatsCollectorFactory.init(configuration);
    }

    @AfterMethod
    public void cleanUp() {
        ClusterMetaStore.close();
        AbstractScheduledWorkerManager.closeInstance();
    }

    @Test
    public void testBatch() {
        ClusterMetaStore.init(0, "driver-0", configuration);
        PipelineCycleScheduler scheduler = new PipelineCycleScheduler();
        processor.register(scheduler);

        RedoSchedulerContext context =
            (RedoSchedulerContext) CycleSchedulerContextFactory.create(buildMockCycle(false, 1), null);
        context.setRollback(false);
        PipelineStateMachine stateMachine = new PipelineStateMachine();
        stateMachine.init(context);

        // START -> INIT.
        IScheduleState state = stateMachine.transition();
        Assert.assertEquals(ScheduleStateType.COMPOSE, state.getScheduleStateType());
        Assert.assertEquals(ScheduleStateType.INIT, ((ComposeState) state).getStates().get(0).getScheduleStateType());
        Assert.assertEquals(ScheduleStateType.EXECUTE_COMPUTE,
            ((ComposeState) state).getStates().get(1).getScheduleStateType());
        context.getNextIterationId();

        state = stateMachine.transition();
        Assert.assertEquals(null, state);
        while (context.hasNextToFinish()) {
            context.getNextFinishIterationId();
        }

        // EXECUTE_COMPUTE -> CLEAN_CYCLE.
        state = stateMachine.transition();
        Assert.assertEquals(ScheduleStateType.CLEAN_CYCLE, state.getScheduleStateType());

        // ITERATION_FINISH -> END.
        Assert.assertEquals(ScheduleStateType.END, stateMachine.getCurrentState().getScheduleStateType());
    }

    @Test
    public void testStream() {
        configuration.put(CLUSTER_ID, "restart");
        ClusterMetaStore.init(0, "driver-0", configuration);
        PipelineCycleScheduler scheduler = new PipelineCycleScheduler();
        processor.register(scheduler);

        CheckpointSchedulerContext context =
            (CheckpointSchedulerContext) CycleSchedulerContextFactory.create(buildMockCycle(false), null);
        PipelineStateMachine stateMachine = new PipelineStateMachine();
        stateMachine.init(context);

        // START -> INIT.
        IScheduleState state = stateMachine.transition();
        Assert.assertEquals(ScheduleStateType.COMPOSE, state.getScheduleStateType());
        Assert.assertEquals(ScheduleStateType.INIT, ((ComposeState) state).getStates().get(0).getScheduleStateType());
        Assert.assertEquals(ScheduleStateType.EXECUTE_COMPUTE,
            ((ComposeState) state).getStates().get(1).getScheduleStateType());

        // INIT -> loop (EXECUTE_COMPUTE).
        for (int i = 1; i <= 5; i++) {
            state = stateMachine.transition();
            context.getNextIterationId();
            Assert.assertEquals(ScheduleStateType.EXECUTE_COMPUTE, state.getScheduleStateType());
            state = stateMachine.transition();
            Assert.assertEquals(null, state);
            while (context.hasNextToFinish()) {
                context.getNextFinishIterationId();
            }
        }

        // EXECUTE_COMPUTE -> CLEAN_CYCLE.
        state = stateMachine.transition();
        Assert.assertEquals(ScheduleStateType.CLEAN_CYCLE, state.getScheduleStateType());

        // ITERATION_FINISH -> END.
        Assert.assertEquals(ScheduleStateType.END, stateMachine.getCurrentState().getScheduleStateType());
    }

    @Test
    public void testIteration() {
        ClusterMetaStore.init(0, "driver-0", configuration);
        PipelineCycleScheduler scheduler = new PipelineCycleScheduler();
        processor.register(scheduler);

        RedoSchedulerContext parentContext = new RedoSchedulerContext(buildMockCycle(false), null);
        parentContext.init(1);

        IterationRedoSchedulerContext context =
            (IterationRedoSchedulerContext) CycleSchedulerContextFactory.create(buildMockCycle(true), parentContext);
        PipelineStateMachine stateMachine = new PipelineStateMachine();
        stateMachine.init(context);

        // START -> INIT.
        IScheduleState state = stateMachine.transition();
        Assert.assertEquals(ScheduleStateType.COMPOSE, state.getScheduleStateType());
        Assert.assertEquals(ScheduleStateType.INIT, ((ComposeState) state).getStates().get(0).getScheduleStateType());
        Assert.assertEquals(ScheduleStateType.ITERATION_INIT,
            ((ComposeState) state).getStates().get(1).getScheduleStateType());

        // ITERATION_INIT -> loop (EXECUTE_COMPUTE).
        for (int i = 1; i <= 5; i++) {
            state = stateMachine.transition();
            context.getNextIterationId();
            Assert.assertEquals(ScheduleStateType.EXECUTE_COMPUTE, state.getScheduleStateType());
            state = stateMachine.transition();
            Assert.assertEquals(null, state);
            while (context.hasNextToFinish()) {
                context.getNextFinishIterationId();
            }
        }

        // EXECUTE_COMPUTE -> ITERATION_FINISH.
        state = stateMachine.transition();
        Assert.assertEquals(ScheduleStateType.COMPOSE, state.getScheduleStateType());
        Assert.assertEquals(ScheduleStateType.ITERATION_FINISH, ((ComposeState) state).getStates().get(0).getScheduleStateType());
        Assert.assertEquals(ScheduleStateType.CLEAN_CYCLE,
            ((ComposeState) state).getStates().get(1).getScheduleStateType());

        // ITERATION_FINISH -> END.
        Assert.assertEquals(ScheduleStateType.END, stateMachine.getCurrentState().getScheduleStateType());
    }

    @Test
    public void testRollback001() {
        ClusterMetaStore.init(0, "driver-0", configuration);
        PipelineCycleScheduler scheduler = new PipelineCycleScheduler();
        processor.register(scheduler);

        CheckpointSchedulerContext context =
            (CheckpointSchedulerContext) CycleSchedulerContextFactory.create(buildMockCycle(false), null);
        PipelineStateMachine stateMachine = new PipelineStateMachine();

        context.init(2);
        context.setRecovered(true);
        stateMachine.init(context);

        // START -> ROLLBACK.
        IScheduleState state = stateMachine.transition();
        Assert.assertEquals(ScheduleStateType.COMPOSE, state.getScheduleStateType());
        Assert.assertEquals(ScheduleStateType.ROLLBACK,
            ((ComposeState) state).getStates().get(0).getScheduleStateType());
        Assert.assertEquals(ScheduleStateType.EXECUTE_COMPUTE,
            ((ComposeState) state).getStates().get(1).getScheduleStateType());

        // ROLLBACK -> loop (EXECUTE_COMPUTE).
        for (int i = 1; i <= 4; i++) {
            state = stateMachine.transition();
            context.getNextIterationId();
            Assert.assertEquals(ScheduleStateType.EXECUTE_COMPUTE, state.getScheduleStateType());
            state = stateMachine.transition();
            Assert.assertEquals(null, state);
            while (context.hasNextToFinish()) {
                context.getNextFinishIterationId();
            }
        }

        // EXECUTE_COMPUTE -> ITERATION_FINISH.
        state = stateMachine.transition();
        Assert.assertEquals(ScheduleStateType.CLEAN_CYCLE, state.getScheduleStateType());

        // ITERATION_FINISH -> END.
        Assert.assertEquals(ScheduleStateType.END, stateMachine.getCurrentState().getScheduleStateType());
    }

    @Test
    public void testRollback002() {
        ClusterMetaStore.init(0, "driver-0", configuration);
        PipelineCycleScheduler scheduler = new PipelineCycleScheduler();
        processor.register(scheduler);

        CheckpointSchedulerContext context =
            (CheckpointSchedulerContext) CycleSchedulerContextFactory.create(buildMockCycle(false), null);
        PipelineStateMachine stateMachine = new PipelineStateMachine();

        context.init(3);
        context.setRollback(true);
        stateMachine.init(context);

        // START -> INIT.
        IScheduleState state = stateMachine.transition();
        Assert.assertEquals(ScheduleStateType.COMPOSE, state.getScheduleStateType());
        Assert.assertEquals(ScheduleStateType.INIT,
            ((ComposeState) state).getStates().get(0).getScheduleStateType());
        Assert.assertEquals(ScheduleStateType.ROLLBACK,
            ((ComposeState) state).getStates().get(1).getScheduleStateType());
        Assert.assertEquals(ScheduleStateType.EXECUTE_COMPUTE,
            ((ComposeState) state).getStates().get(2).getScheduleStateType());

        // ROLLBACK -> loop (EXECUTE_COMPUTE).
        for (int i = 1; i <= 3; i++) {
            state = stateMachine.transition();
            context.getNextIterationId();
            Assert.assertEquals(ScheduleStateType.EXECUTE_COMPUTE, state.getScheduleStateType());
            state = stateMachine.transition();
            Assert.assertEquals(null, state);
            while (context.hasNextToFinish()) {
                context.getNextFinishIterationId();
            }
        }

        // EXECUTE_COMPUTE -> ITERATION_FINISH.
        state = stateMachine.transition();
        Assert.assertEquals(ScheduleStateType.CLEAN_CYCLE, state.getScheduleStateType());

        // ITERATION_FINISH -> END.
        Assert.assertEquals(ScheduleStateType.END, stateMachine.getCurrentState().getScheduleStateType());
        context.setRollback(false);
    }

    private ExecutionNodeCycle buildMockCycle(boolean isIterative, long finishIterationId) {
        Configuration configuration = new Configuration();
        configuration.put(JOB_UNIQUE_ID, "test-scheduler-context");
        configuration.put(SYSTEM_STATE_BACKEND_TYPE.getKey(), StoreType.MEMORY.name());
        ClusterMetaStore.init(0, "driver-0", configuration);

        ExecutionVertexGroup vertexGroup = new ExecutionVertexGroup(1);
        vertexGroup.getCycleGroupMeta().setFlyingCount(1);
        vertexGroup.getCycleGroupMeta().setIterationCount(finishIterationId);
        if (isIterative) {
            vertexGroup.getCycleGroupMeta().setGroupType(CycleGroupType.incremental);
        } else {
            vertexGroup.getCycleGroupMeta().setGroupType(CycleGroupType.pipelined);
        }
        ExecutionVertex vertex = new ExecutionVertex(0, "test");
        vertex.setParallelism(2);
        vertexGroup.getVertexMap().put(0, vertex);

        return new ExecutionNodeCycle(0, 0, 0, "test", vertexGroup, configuration, "driver_id", 0);
    }

    private ExecutionNodeCycle buildMockCycle(boolean isIterative) {
        return buildMockCycle(isIterative, 5);
    }

}
