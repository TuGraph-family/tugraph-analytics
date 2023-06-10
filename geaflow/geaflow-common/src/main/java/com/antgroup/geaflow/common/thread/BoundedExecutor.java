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

package com.antgroup.geaflow.common.thread;

import com.antgroup.geaflow.common.exception.GeaflowRuntimeException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BoundedExecutor extends ThreadPoolExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BoundedExecutor.class);
    private static final int DEFAULT_KEEP_ALIVE_MINUTES = 30;

    private final Semaphore semaphore;
    private final int counter;

    public BoundedExecutor(int bound, int capacity) {
        this(bound, capacity, DEFAULT_KEEP_ALIVE_MINUTES, TimeUnit.MINUTES);
    }

    public BoundedExecutor(int bound, int capacity, long keepAliveTime, TimeUnit unit) {
        super(bound, bound, keepAliveTime, unit, new LinkedBlockingQueue<>(capacity));
        counter = capacity + bound;
        semaphore = new Semaphore(counter);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        semaphore.release();
    }

    public void tryExecute(Runnable command) {
        while (true) {
            try {
                semaphore.acquire();
                super.execute(command);
                break;
            } catch (RejectedExecutionException e) {
                LOGGER.info("reject task, retry to submit");
                semaphore.release();
                continue;
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
                throw new GeaflowRuntimeException(e);
            }
        }
    }

    @Override
    public Future<?> submit(Runnable task) {
        try {
            semaphore.acquire();
            return super.submit(task);
        } catch (RejectedExecutionException e) {
            LOGGER.error(e.getMessage(), e);
            semaphore.release();
            throw e;
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
            throw new GeaflowRuntimeException(e);
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        while (true) {
            try {
                semaphore.acquire();
                return super.submit(task);
            } catch (RejectedExecutionException e) {
                LOGGER.info("reject task, retry to submit");
                semaphore.release();
                continue;
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
                throw new GeaflowRuntimeException(e);
            }
        }
    }

    public boolean isEmpty() {
        LOGGER.info("current available:{}, counter:{}",
            this.semaphore.availablePermits(), counter);
        return this.semaphore.availablePermits() == counter;
    }

}
