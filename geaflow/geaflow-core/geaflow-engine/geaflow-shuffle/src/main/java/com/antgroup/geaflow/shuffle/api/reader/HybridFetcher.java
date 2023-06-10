/*
 * Copyright 2023 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.antgroup.geaflow.shuffle.api.reader;

import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.common.metric.ShuffleReadMetrics;
import com.antgroup.geaflow.shuffle.message.BaseSliceMeta;
import com.antgroup.geaflow.shuffle.message.ISliceMeta;
import com.antgroup.geaflow.shuffle.message.PipelineBarrier;
import com.antgroup.geaflow.shuffle.message.PipelineEvent;
import com.antgroup.geaflow.shuffle.network.IConnectionManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HybridFetcher extends AbstractFetcher {

    private static final Logger LOG = LoggerFactory.getLogger(HybridFetcher.class);

    protected PipelineFetcher pipelineFetcher;
    protected List<IShuffleFetcher> fetcherList;
    protected List<Map<Integer, List<ISliceMeta>>> sliceMapList;
    private Queue<PipelineBarrier> barrierQueue;

    private int curInputIndex;
    private IShuffleFetcher curFetcher;
    private PipelineEvent curValue;
    private FetchContext fetchContext;

    public HybridFetcher() {
    }

    @Override
    public void setup(IConnectionManager connectionManager, Configuration config) {
        super.setup(connectionManager, config);
        this.pipelineFetcher = ShuffleFetcherFactory.getPipelineFetcher(connectionManager, config);
        this.sliceMapList = new ArrayList<>();
        this.fetcherList = new ArrayList<>();
        this.barrierQueue = new LinkedList<>();
    }

    @Override
    public void init(FetchContext fetchContext) {
        super.init(fetchContext);
        this.sliceMapList.clear();
        this.fetcherList.clear();
        this.fetchContext = fetchContext;
        this.curFetcher = null;

        Map<Integer, List<ISliceMeta>> sliceMap = fetchContext.getRequest().getInputSlices();
        if (sliceMap.isEmpty()) {
            readMetrics.setFetchSlices(0);
            return;
        }

        splitRemoteSlices(sliceMap);
        readMetrics.setFetchSlices(totalSliceNum);

        curInputIndex = 0;
        if (sliceMapList.size() > 0) {
            initNextFetcher();
        }
    }

    private void initNextFetcher() {
        Map<Integer, List<ISliceMeta>> sliceMetas = sliceMapList.get(curInputIndex);
        curFetcher = fetcherList.get(curInputIndex);
        fetchContext.setInputSliceMap(sliceMetas);
        curFetcher.init(fetchContext);
    }

    @Override
    public boolean hasNext() {
        if (curValue != null) {
            return true;
        }
        if (curFetcher != null) {
            boolean hasNext = curFetcher.hasNext();
            while (!hasNext && curInputIndex < sliceMapList.size() - 1) {
                curInputIndex++;
                initNextFetcher();
                hasNext = curFetcher.hasNext();
            }
            if (hasNext) {
                curValue = curFetcher.next();
            } else {
                curValue = barrierQueue.poll();
            }
        } else {
            curValue = barrierQueue.poll();
        }
        return curValue != null;
    }

    @Override
    public PipelineEvent next() {
        processedNum++;
        PipelineEvent result = curValue;
        curValue = null;
        return result;
    }

    @Override
    public ShuffleReadMetrics getReadMetrics() {
        if (readMetrics != null) {
            readMetrics.merge(pipelineFetcher.getReadMetrics());
            readMetrics.setFetchSlices(totalSliceNum);
        }
        return readMetrics;
    }

    @Override
    public void close() {
        pipelineFetcher.close();
    }

    protected void splitRemoteSlices(Map<Integer, List<ISliceMeta>> sliceMap) {
        int totalPipelineSlices = 0;
        int totalRemoteSlices = 0;

        Map<Integer, List<ISliceMeta>> pipelineSliceMap = null;
        Map<Integer, List<ISliceMeta>> remoteShuffleSliceMap = null;

        for (Map.Entry<Integer, List<ISliceMeta>> entry : sliceMap.entrySet()) {
            List<ISliceMeta> remoteSlices = new ArrayList<>();
            List<ISliceMeta> pipelineSlices = new ArrayList<>();
            for (ISliceMeta sliceMeta : entry.getValue()) {
                if (sliceMeta.getRecordNum() > 0) {
                    pipelineSlices.add(sliceMeta);
                } else {
                    addToBarrierQueue(sliceMeta);
                }
            }
            if (!pipelineSlices.isEmpty()) {
                totalPipelineSlices += pipelineSlices.size();
                if (pipelineSliceMap == null) {
                    pipelineSliceMap = new HashMap<>();
                }
                pipelineSliceMap.put(entry.getKey(), pipelineSlices);
            }
            if (!remoteSlices.isEmpty()) {
                totalRemoteSlices += remoteSlices.size();
                if (remoteShuffleSliceMap == null) {
                    remoteShuffleSliceMap = new HashMap<>();
                }
                remoteShuffleSliceMap.put(entry.getKey(), remoteSlices);
            }
        }

        totalSliceNum = totalPipelineSlices + totalRemoteSlices;
        if (pipelineSliceMap != null) {
            sliceMapList.add(pipelineSliceMap);
            fetcherList.add(pipelineFetcher);
        }

        LOG.info("{} shards:{} pipelineSlice:{} remoteSlice:{}", taskName, sliceMap.size(),
            totalPipelineSlices, totalRemoteSlices);
    }

    protected void addToBarrierQueue(ISliceMeta sliceMeta) {
        BaseSliceMeta baseSliceMeta = (BaseSliceMeta) sliceMeta;
        PipelineBarrier barrier = new PipelineBarrier(baseSliceMeta.getBatchId(),
            baseSliceMeta.getEdgeId(), sliceMeta.getSourceIndex(), sliceMeta.getTargetIndex(),
            sliceMeta.getRecordNum());
        barrierQueue.add(barrier);
    }

}
