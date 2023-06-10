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

package com.antgroup.geaflow.cluster.config;

import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CLIENT_DISK_GB;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CLIENT_JVM_OPTIONS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CLIENT_MEMORY_MB;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CLIENT_VCORES;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_DISK_GB;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_HEAP_SIZE_MB;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_JVM_OPTION;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_MEMORY_MB;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_NUM;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_VCORES;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.CONTAINER_WORKER_NUM;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.DRIVER_DISK_GB;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.DRIVER_JVM_OPTION;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.DRIVER_MEMORY_MB;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.DRIVER_NUM;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.DRIVER_VCORES;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.FO_ENABLE;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.FO_MAX_RESTARTS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.MASTER_DISK_GB;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.MASTER_JVM_OPTIONS;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.MASTER_MEMORY_MB;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.MASTER_VCORES;
import static com.antgroup.geaflow.common.config.keys.ExecutionConfigKeys.REGISTER_TIMEOUT;

import com.antgroup.geaflow.common.config.Configuration;
import com.google.common.base.Preconditions;
import java.io.Serializable;

public class ClusterConfig implements Serializable {

    public static final String MASTER_ADDRESS = "geaflow.master.address";
    private static final double DEFAULT_HEAP_FRACTION = 0.8;

    private int containerNum;
    private int containerMemoryMB;
    private int containerDiskGB;
    private double containerVcores;
    private int containerWorkerNum;
    private ClusterJvmOptions containerJvmOptions;

    private int masterMemoryMB;
    private int masterDiskGB;
    private double masterVcores;
    private ClusterJvmOptions masterJvmOptions;

    private int driverNum;
    private int driverMemoryMB;
    private int driverDiskGB;
    private double driverVcores;
    private ClusterJvmOptions driverJvmOptions;

    private int clientMemoryMB;
    private int clientDiskGB;
    private double clientVcores;
    private ClusterJvmOptions clientJvmOptions;

    private boolean isFoEnable;
    private int maxRestarts;
    private Configuration config;

    public static ClusterConfig build(Configuration config) {
        ClusterConfig clusterConfig = new ClusterConfig();

        clusterConfig.setMasterMemoryMB(config.getInteger(MASTER_MEMORY_MB));
        clusterConfig.setMasterDiskGB(config.getInteger(MASTER_DISK_GB));
        ClusterJvmOptions masterJvmOptions = ClusterJvmOptions.build(
            config.getString(MASTER_JVM_OPTIONS));
        clusterConfig.setMasterJvmOptions(masterJvmOptions);
        clusterConfig.setMasterVcores(config.getDouble(MASTER_VCORES));

        ClusterJvmOptions clientJvmOptions = ClusterJvmOptions.build(
            config.getString(CLIENT_JVM_OPTIONS));
        clusterConfig.setClientJvmOptions(clientJvmOptions);
        clusterConfig.setClientVcores(config.getDouble(CLIENT_VCORES));
        clusterConfig.setClientMemoryMB(config.getInteger(CLIENT_MEMORY_MB));
        clusterConfig.setClientDiskGB(config.getInteger(CLIENT_DISK_GB));

        int driverMB = config.getInteger(DRIVER_MEMORY_MB);
        clusterConfig.setDriverMemoryMB(driverMB);
        int driverDiskGB = config.getInteger(DRIVER_DISK_GB);
        clusterConfig.setDriverDiskGB(driverDiskGB);
        ClusterJvmOptions driverJvmOptions = ClusterJvmOptions.build(
            config.getString(DRIVER_JVM_OPTION));
        clusterConfig.setDriverJvmOptions(driverJvmOptions);
        clusterConfig.setDriverVcores(config.getDouble(DRIVER_VCORES));

        int driverNum = config.getInteger(DRIVER_NUM);
        Preconditions.checkArgument(driverNum == 1, "only one driver is allowed");
        clusterConfig.setDriverNum(driverNum);

        clusterConfig.setContainerMemoryMB(config.getInteger(CONTAINER_MEMORY_MB));
        clusterConfig.setContainerDiskGB(config.getInteger(CONTAINER_DISK_GB));
        clusterConfig.setContainerVcores(config.getDouble(CONTAINER_VCORES));
        int workersPerContainer = config.getInteger(CONTAINER_WORKER_NUM);
        clusterConfig.setContainerWorkerNum(workersPerContainer);

        int containerNum = config.getInteger(CONTAINER_NUM);
        clusterConfig.setContainerNum(containerNum);

        ClusterJvmOptions containerJvmOptions;
        if (config.contains(CONTAINER_JVM_OPTION)) {
            containerJvmOptions = ClusterJvmOptions.build(config.getString(CONTAINER_JVM_OPTION));
        } else {
            containerJvmOptions = new ClusterJvmOptions();
            containerJvmOptions.setMaxHeapMB((int) (driverMB * DEFAULT_HEAP_FRACTION));
        }
        clusterConfig.setContainerJvmOptions(containerJvmOptions);
        config.put(CONTAINER_HEAP_SIZE_MB, String.valueOf(containerJvmOptions.getMaxHeapMB()));

        clusterConfig.setFoEnable(config.getBoolean(FO_ENABLE));

        // set fo_max_restarts to 0 if FO disabled
        clusterConfig.setMaxRestarts(config.getInteger(FO_MAX_RESTARTS));
        clusterConfig.setConfig(config);

        return clusterConfig;
    }

    public int getContainerNum() {
        return containerNum;
    }

    public void setContainerNum(int containerNum) {
        this.containerNum = containerNum;
    }

    public int getContainerMemoryMB() {
        return containerMemoryMB;
    }

    public void setContainerMemoryMB(int containerMemoryMB) {
        this.containerMemoryMB = containerMemoryMB;
    }

    public int getContainerDiskGB() {
        return containerDiskGB;
    }

    public void setContainerDiskGB(int containerDiskGB) {
        this.containerDiskGB = containerDiskGB;
    }

    public int getContainerWorkerNum() {
        return containerWorkerNum;
    }

    public void setContainerWorkerNum(int containerWorkerNum) {
        this.containerWorkerNum = containerWorkerNum;
    }

    public double getContainerVcores() {
        return containerVcores;
    }

    public void setContainerVcores(double containerVcores) {
        this.containerVcores = containerVcores;
    }

    public int getMasterMemoryMB() {
        return masterMemoryMB;
    }

    public void setMasterMemoryMB(int masterMemoryMB) {
        this.masterMemoryMB = masterMemoryMB;
    }

    public int getDriverMemoryMB() {
        return driverMemoryMB;
    }

    public void setDriverMemoryMB(int driverMemoryMB) {
        this.driverMemoryMB = driverMemoryMB;
    }

    public int getMasterDiskGB() {
        return masterDiskGB;
    }

    public void setMasterDiskGB(int masterDiskGB) {
        this.masterDiskGB = masterDiskGB;
    }

    public int getDriverDiskGB() {
        return driverDiskGB;
    }

    public void setDriverDiskGB(int driverDiskGB) {
        this.driverDiskGB = driverDiskGB;
    }

    public ClusterJvmOptions getDriverJvmOptions() {
        return driverJvmOptions;
    }

    public void setDriverJvmOptions(ClusterJvmOptions driverJvmOptions) {
        this.driverJvmOptions = driverJvmOptions;
    }

    public ClusterJvmOptions getMasterJvmOptions() {
        return masterJvmOptions;
    }

    public void setMasterJvmOptions(ClusterJvmOptions masterJvmOptions) {
        this.masterJvmOptions = masterJvmOptions;
    }

    public double getMasterVcores() {
        return masterVcores;
    }

    public void setMasterVcores(double masterVcores) {
        this.masterVcores = masterVcores;
    }

    public double getDriverVcores() {
        return driverVcores;
    }

    public void setDriverVcores(double driverVcores) {
        this.driverVcores = driverVcores;
    }

    public Configuration getConfig() {
        return config;
    }

    public void setConfig(Configuration config) {
        this.config = config;
    }

    public boolean isFoEnable() {
        return isFoEnable;
    }

    public void setFoEnable(boolean isFoEnable) {
        this.isFoEnable = isFoEnable;
    }

    public int getMaxRestarts() {
        return maxRestarts;
    }

    public void setMaxRestarts(int maxRestarts) {
        this.maxRestarts = maxRestarts;
    }

    public int getDriverNum() {
        return driverNum;
    }

    public void setDriverNum(int driverNum) {
        this.driverNum = driverNum;
    }

    public ClusterJvmOptions getContainerJvmOptions() {
        return containerJvmOptions;
    }

    public void setContainerJvmOptions(ClusterJvmOptions containerJvmOptions) {
        this.containerJvmOptions = containerJvmOptions;
    }

    public ClusterJvmOptions getClientJvmOptions() {
        return clientJvmOptions;
    }

    public void setClientJvmOptions(ClusterJvmOptions clientJvmOptions) {
        this.clientJvmOptions = clientJvmOptions;
    }

    public double getClientVcores() {
        return clientVcores;
    }

    public void setClientVcores(double clientVcores) {
        this.clientVcores = clientVcores;
    }

    public int getClientMemoryMB() {
        return clientMemoryMB;
    }

    public void setClientMemoryMB(int clientMemoryMB) {
        this.clientMemoryMB = clientMemoryMB;
    }

    public int getClientDiskGB() {
        return clientDiskGB;
    }

    public void setClientDiskGB(int clientDiskGB) {
        this.clientDiskGB = clientDiskGB;
    }

    public int getDriverRegisterTimeoutSec() {
        return config.getInteger(REGISTER_TIMEOUT);
    }

    @Override
    public String toString() {
        return "ClusterConfig{" + "containerNum=" + containerNum + ", containerMemoryMB="
            + containerMemoryMB + ", containerWorkers=" + containerWorkerNum + ", "
            + "containerJvmOptions=" + containerJvmOptions + ", masterMemoryMB=" + masterMemoryMB
            + ", masterJvmOptions=" + masterJvmOptions + ", driverMemoryMB=" + driverMemoryMB
            + ", driverJvmOptions=" + driverJvmOptions + ", restartAllFo="
            + isFoEnable + '}';
    }
}
