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

package com.antgroup.geaflow.rpc.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 0.15.0)",
    comments = "Source: master.proto")
public class MasterServiceGrpc {

  private MasterServiceGrpc() {}

  public static final String SERVICE_NAME = "MasterService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.antgroup.geaflow.rpc.proto.Master.RegisterRequest,
      com.antgroup.geaflow.rpc.proto.Master.RegisterResponse> METHOD_REGISTER_CONTAINER =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "MasterService", "registerContainer"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.antgroup.geaflow.rpc.proto.Master.RegisterRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.antgroup.geaflow.rpc.proto.Master.RegisterResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest,
      com.google.protobuf.Empty> METHOD_RECEIVE_HEARTBEAT =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "MasterService", "receiveHeartbeat"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest,
      com.google.protobuf.Empty> METHOD_RECEIVE_EXCEPTION =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "MasterService", "receiveException"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<com.google.protobuf.Empty,
      com.google.protobuf.Empty> METHOD_CLOSE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "MasterService", "close"),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.Empty.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MasterServiceStub newStub(io.grpc.Channel channel) {
    return new MasterServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MasterServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new MasterServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static MasterServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new MasterServiceFutureStub(channel);
  }

  /**
   */
  @java.lang.Deprecated public static interface MasterService {

    /**
     * <pre>
     *提交注册container/driver
     * </pre>
     */
    public void registerContainer(com.antgroup.geaflow.rpc.proto.Master.RegisterRequest request,
        io.grpc.stub.StreamObserver<com.antgroup.geaflow.rpc.proto.Master.RegisterResponse> responseObserver);

    /**
     * <pre>
     *处理executor/driver发送的心跳信息
     * </pre>
     */
    public void receiveHeartbeat(com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);

    /**
     * <pre>
     *处理executor/driver发送的异常信息
     * </pre>
     */
    public void receiveException(com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);

    /**
     * <pre>
     * 关闭结束master
     * </pre>
     */
    public void close(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver);
  }

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1469")
  public static abstract class MasterServiceImplBase implements MasterService, io.grpc.BindableService {

    @java.lang.Override
    public void registerContainer(com.antgroup.geaflow.rpc.proto.Master.RegisterRequest request,
        io.grpc.stub.StreamObserver<com.antgroup.geaflow.rpc.proto.Master.RegisterResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_REGISTER_CONTAINER, responseObserver);
    }

    @java.lang.Override
    public void receiveHeartbeat(com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_RECEIVE_HEARTBEAT, responseObserver);
    }

    @java.lang.Override
    public void receiveException(com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_RECEIVE_EXCEPTION, responseObserver);
    }

    @java.lang.Override
    public void close(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CLOSE, responseObserver);
    }

    @java.lang.Override public io.grpc.ServerServiceDefinition bindService() {
      return MasterServiceGrpc.bindService(this);
    }
  }

  /**
   */
  @java.lang.Deprecated public static interface MasterServiceBlockingClient {

    /**
     * <pre>
     *提交注册container/driver
     * </pre>
     */
    public com.antgroup.geaflow.rpc.proto.Master.RegisterResponse registerContainer(com.antgroup.geaflow.rpc.proto.Master.RegisterRequest request);

    /**
     * <pre>
     *处理executor/driver发送的心跳信息
     * </pre>
     */
    public com.google.protobuf.Empty receiveHeartbeat(com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest request);

    /**
     * <pre>
     *处理executor/driver发送的异常信息
     * </pre>
     */
    public com.google.protobuf.Empty receiveException(com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest request);

    /**
     * <pre>
     * 关闭结束master
     * </pre>
     */
    public com.google.protobuf.Empty close(com.google.protobuf.Empty request);
  }

  /**
   */
  @java.lang.Deprecated public static interface MasterServiceFutureClient {

    /**
     * <pre>
     *提交注册container/driver
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.antgroup.geaflow.rpc.proto.Master.RegisterResponse> registerContainer(
        com.antgroup.geaflow.rpc.proto.Master.RegisterRequest request);

    /**
     * <pre>
     *处理executor/driver发送的心跳信息
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> receiveHeartbeat(
        com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest request);

    /**
     * <pre>
     *处理executor/driver发送的异常信息
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> receiveException(
        com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest request);

    /**
     * <pre>
     * 关闭结束master
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> close(
        com.google.protobuf.Empty request);
  }

  public static class MasterServiceStub extends io.grpc.stub.AbstractStub<MasterServiceStub>
      implements MasterService {
    private MasterServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MasterServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MasterServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MasterServiceStub(channel, callOptions);
    }

    @java.lang.Override
    public void registerContainer(com.antgroup.geaflow.rpc.proto.Master.RegisterRequest request,
        io.grpc.stub.StreamObserver<com.antgroup.geaflow.rpc.proto.Master.RegisterResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_REGISTER_CONTAINER, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void receiveHeartbeat(com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_RECEIVE_HEARTBEAT, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void receiveException(com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_RECEIVE_EXCEPTION, getCallOptions()), request, responseObserver);
    }

    @java.lang.Override
    public void close(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CLOSE, getCallOptions()), request, responseObserver);
    }
  }

  public static class MasterServiceBlockingStub extends io.grpc.stub.AbstractStub<MasterServiceBlockingStub>
      implements MasterServiceBlockingClient {
    private MasterServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MasterServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MasterServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MasterServiceBlockingStub(channel, callOptions);
    }

    @java.lang.Override
    public com.antgroup.geaflow.rpc.proto.Master.RegisterResponse registerContainer(com.antgroup.geaflow.rpc.proto.Master.RegisterRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_REGISTER_CONTAINER, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty receiveHeartbeat(com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_RECEIVE_HEARTBEAT, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty receiveException(com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_RECEIVE_EXCEPTION, getCallOptions(), request);
    }

    @java.lang.Override
    public com.google.protobuf.Empty close(com.google.protobuf.Empty request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CLOSE, getCallOptions(), request);
    }
  }

  public static class MasterServiceFutureStub extends io.grpc.stub.AbstractStub<MasterServiceFutureStub>
      implements MasterServiceFutureClient {
    private MasterServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private MasterServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MasterServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new MasterServiceFutureStub(channel, callOptions);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.antgroup.geaflow.rpc.proto.Master.RegisterResponse> registerContainer(
        com.antgroup.geaflow.rpc.proto.Master.RegisterRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_REGISTER_CONTAINER, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> receiveHeartbeat(
        com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_RECEIVE_HEARTBEAT, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> receiveException(
        com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_RECEIVE_EXCEPTION, getCallOptions()), request);
    }

    @java.lang.Override
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> close(
        com.google.protobuf.Empty request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CLOSE, getCallOptions()), request);
    }
  }

  @java.lang.Deprecated public static abstract class AbstractMasterService extends MasterServiceImplBase {}

  private static final int METHODID_REGISTER_CONTAINER = 0;
  private static final int METHODID_RECEIVE_HEARTBEAT = 1;
  private static final int METHODID_RECEIVE_EXCEPTION = 2;
  private static final int METHODID_CLOSE = 3;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final MasterService serviceImpl;
    private final int methodId;

    public MethodHandlers(MasterService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_REGISTER_CONTAINER:
          serviceImpl.registerContainer((com.antgroup.geaflow.rpc.proto.Master.RegisterRequest) request,
              (io.grpc.stub.StreamObserver<com.antgroup.geaflow.rpc.proto.Master.RegisterResponse>) responseObserver);
          break;
        case METHODID_RECEIVE_HEARTBEAT:
          serviceImpl.receiveHeartbeat((com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_RECEIVE_EXCEPTION:
          serviceImpl.receiveException((com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        case METHODID_CLOSE:
          serviceImpl.close((com.google.protobuf.Empty) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    return new io.grpc.ServiceDescriptor(SERVICE_NAME,
        METHOD_REGISTER_CONTAINER,
        METHOD_RECEIVE_HEARTBEAT,
        METHOD_RECEIVE_EXCEPTION,
        METHOD_CLOSE);
  }

  @java.lang.Deprecated public static io.grpc.ServerServiceDefinition bindService(
      final MasterService serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          METHOD_REGISTER_CONTAINER,
          asyncUnaryCall(
            new MethodHandlers<
              com.antgroup.geaflow.rpc.proto.Master.RegisterRequest,
              com.antgroup.geaflow.rpc.proto.Master.RegisterResponse>(
                serviceImpl, METHODID_REGISTER_CONTAINER)))
        .addMethod(
          METHOD_RECEIVE_HEARTBEAT,
          asyncUnaryCall(
            new MethodHandlers<
              com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest,
              com.google.protobuf.Empty>(
                serviceImpl, METHODID_RECEIVE_HEARTBEAT)))
        .addMethod(
          METHOD_RECEIVE_EXCEPTION,
          asyncUnaryCall(
            new MethodHandlers<
              com.antgroup.geaflow.rpc.proto.Master.HeartbeatRequest,
              com.google.protobuf.Empty>(
                serviceImpl, METHODID_RECEIVE_EXCEPTION)))
        .addMethod(
          METHOD_CLOSE,
          asyncUnaryCall(
            new MethodHandlers<
              com.google.protobuf.Empty,
              com.google.protobuf.Empty>(
                serviceImpl, METHODID_CLOSE)))
        .build();
  }
}
