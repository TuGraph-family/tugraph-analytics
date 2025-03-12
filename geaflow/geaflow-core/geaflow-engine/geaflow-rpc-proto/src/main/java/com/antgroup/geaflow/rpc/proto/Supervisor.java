/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: supervisor.proto

package com.antgroup.geaflow.rpc.proto;

public final class Supervisor {
  private Supervisor() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface RestartRequestOrBuilder extends
      // @@protoc_insertion_point(interface_extends:RestartRequest)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>int32 pid = 1;</code>
     * @return The pid.
     */
    int getPid();
  }
  /**
   * Protobuf type {@code RestartRequest}
   */
  public static final class RestartRequest extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:RestartRequest)
      RestartRequestOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use RestartRequest.newBuilder() to construct.
    private RestartRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private RestartRequest() {
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new RestartRequest();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private RestartRequest(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            case 8: {

              pid_ = input.readInt32();
              break;
            }
            default: {
              if (!parseUnknownField(
                  input, unknownFields, extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.antgroup.geaflow.rpc.proto.Supervisor.internal_static_RestartRequest_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.antgroup.geaflow.rpc.proto.Supervisor.internal_static_RestartRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest.class, com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest.Builder.class);
    }

    public static final int PID_FIELD_NUMBER = 1;
    private int pid_;
    /**
     * <code>int32 pid = 1;</code>
     * @return The pid.
     */
    @java.lang.Override
    public int getPid() {
      return pid_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (pid_ != 0) {
        output.writeInt32(1, pid_);
      }
      unknownFields.writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (pid_ != 0) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(1, pid_);
      }
      size += unknownFields.getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest)) {
        return super.equals(obj);
      }
      com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest other = (com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest) obj;

      if (getPid()
          != other.getPid()) return false;
      if (!unknownFields.equals(other.unknownFields)) return false;
      return true;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      hash = (37 * hash) + PID_FIELD_NUMBER;
      hash = (53 * hash) + getPid();
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    @java.lang.Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code RestartRequest}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:RestartRequest)
        com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.antgroup.geaflow.rpc.proto.Supervisor.internal_static_RestartRequest_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.antgroup.geaflow.rpc.proto.Supervisor.internal_static_RestartRequest_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest.class, com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest.Builder.class);
      }

      // Construct using com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        pid_ = 0;

        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.antgroup.geaflow.rpc.proto.Supervisor.internal_static_RestartRequest_descriptor;
      }

      @java.lang.Override
      public com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest getDefaultInstanceForType() {
        return com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest.getDefaultInstance();
      }

      @java.lang.Override
      public com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest build() {
        com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest buildPartial() {
        com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest result = new com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest(this);
        result.pid_ = pid_;
        onBuilt();
        return result;
      }

      @java.lang.Override
      public Builder clone() {
        return super.clone();
      }
      @java.lang.Override
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.setField(field, value);
      }
      @java.lang.Override
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return super.clearField(field);
      }
      @java.lang.Override
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return super.clearOneof(oneof);
      }
      @java.lang.Override
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, java.lang.Object value) {
        return super.setRepeatedField(field, index, value);
      }
      @java.lang.Override
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.addRepeatedField(field, value);
      }
      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest) {
          return mergeFrom((com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest other) {
        if (other == com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest.getDefaultInstance()) return this;
        if (other.getPid() != 0) {
          setPid(other.getPid());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }

      private int pid_ ;
      /**
       * <code>int32 pid = 1;</code>
       * @return The pid.
       */
      @java.lang.Override
      public int getPid() {
        return pid_;
      }
      /**
       * <code>int32 pid = 1;</code>
       * @param value The pid to set.
       * @return This builder for chaining.
       */
      public Builder setPid(int value) {
        
        pid_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>int32 pid = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearPid() {
        
        pid_ = 0;
        onChanged();
        return this;
      }
      @java.lang.Override
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      @java.lang.Override
      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:RestartRequest)
    }

    // @@protoc_insertion_point(class_scope:RestartRequest)
    private static final com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest();
    }

    public static com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<RestartRequest>
        PARSER = new com.google.protobuf.AbstractParser<RestartRequest>() {
      @java.lang.Override
      public RestartRequest parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new RestartRequest(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<RestartRequest> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<RestartRequest> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public com.antgroup.geaflow.rpc.proto.Supervisor.RestartRequest getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface StatusResponseOrBuilder extends
      // @@protoc_insertion_point(interface_extends:StatusResponse)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>bool isAlive = 1;</code>
     * @return The isAlive.
     */
    boolean getIsAlive();
  }
  /**
   * Protobuf type {@code StatusResponse}
   */
  public static final class StatusResponse extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:StatusResponse)
      StatusResponseOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use StatusResponse.newBuilder() to construct.
    private StatusResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private StatusResponse() {
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new StatusResponse();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private StatusResponse(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            case 8: {

              isAlive_ = input.readBool();
              break;
            }
            default: {
              if (!parseUnknownField(
                  input, unknownFields, extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return com.antgroup.geaflow.rpc.proto.Supervisor.internal_static_StatusResponse_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return com.antgroup.geaflow.rpc.proto.Supervisor.internal_static_StatusResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse.class, com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse.Builder.class);
    }

    public static final int ISALIVE_FIELD_NUMBER = 1;
    private boolean isAlive_;
    /**
     * <code>bool isAlive = 1;</code>
     * @return The isAlive.
     */
    @java.lang.Override
    public boolean getIsAlive() {
      return isAlive_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (isAlive_ != false) {
        output.writeBool(1, isAlive_);
      }
      unknownFields.writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (isAlive_ != false) {
        size += com.google.protobuf.CodedOutputStream
          .computeBoolSize(1, isAlive_);
      }
      size += unknownFields.getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse)) {
        return super.equals(obj);
      }
      com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse other = (com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse) obj;

      if (getIsAlive()
          != other.getIsAlive()) return false;
      if (!unknownFields.equals(other.unknownFields)) return false;
      return true;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      hash = (37 * hash) + ISALIVE_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
          getIsAlive());
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    @java.lang.Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code StatusResponse}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:StatusResponse)
        com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return com.antgroup.geaflow.rpc.proto.Supervisor.internal_static_StatusResponse_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return com.antgroup.geaflow.rpc.proto.Supervisor.internal_static_StatusResponse_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse.class, com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse.Builder.class);
      }

      // Construct using com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        isAlive_ = false;

        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return com.antgroup.geaflow.rpc.proto.Supervisor.internal_static_StatusResponse_descriptor;
      }

      @java.lang.Override
      public com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse getDefaultInstanceForType() {
        return com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse.getDefaultInstance();
      }

      @java.lang.Override
      public com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse build() {
        com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse buildPartial() {
        com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse result = new com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse(this);
        result.isAlive_ = isAlive_;
        onBuilt();
        return result;
      }

      @java.lang.Override
      public Builder clone() {
        return super.clone();
      }
      @java.lang.Override
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.setField(field, value);
      }
      @java.lang.Override
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return super.clearField(field);
      }
      @java.lang.Override
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return super.clearOneof(oneof);
      }
      @java.lang.Override
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, java.lang.Object value) {
        return super.setRepeatedField(field, index, value);
      }
      @java.lang.Override
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.addRepeatedField(field, value);
      }
      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse) {
          return mergeFrom((com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse other) {
        if (other == com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse.getDefaultInstance()) return this;
        if (other.getIsAlive() != false) {
          setIsAlive(other.getIsAlive());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }

      private boolean isAlive_ ;
      /**
       * <code>bool isAlive = 1;</code>
       * @return The isAlive.
       */
      @java.lang.Override
      public boolean getIsAlive() {
        return isAlive_;
      }
      /**
       * <code>bool isAlive = 1;</code>
       * @param value The isAlive to set.
       * @return This builder for chaining.
       */
      public Builder setIsAlive(boolean value) {
        
        isAlive_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>bool isAlive = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearIsAlive() {
        
        isAlive_ = false;
        onChanged();
        return this;
      }
      @java.lang.Override
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      @java.lang.Override
      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:StatusResponse)
    }

    // @@protoc_insertion_point(class_scope:StatusResponse)
    private static final com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse();
    }

    public static com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<StatusResponse>
        PARSER = new com.google.protobuf.AbstractParser<StatusResponse>() {
      @java.lang.Override
      public StatusResponse parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new StatusResponse(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<StatusResponse> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<StatusResponse> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public com.antgroup.geaflow.rpc.proto.Supervisor.StatusResponse getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_RestartRequest_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_RestartRequest_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_StatusResponse_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_StatusResponse_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\020supervisor.proto\032\033google/protobuf/empt" +
      "y.proto\"\035\n\016RestartRequest\022\013\n\003pid\030\001 \001(\005\"!" +
      "\n\016StatusResponse\022\017\n\007isAlive\030\001 \001(\0102~\n\021Sup" +
      "ervisorService\0224\n\007restart\022\017.RestartReque" +
      "st\032\026.google.protobuf.Empty\"\000\0223\n\006status\022\026" +
      ".google.protobuf.Empty\032\017.StatusResponse\"" +
      "\000B\"\n\036com.antgroup.geaflow.rpc.protoP\000b\006p" +
      "roto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          com.google.protobuf.EmptyProto.getDescriptor(),
        });
    internal_static_RestartRequest_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_RestartRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_RestartRequest_descriptor,
        new java.lang.String[] { "Pid", });
    internal_static_StatusResponse_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_StatusResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_StatusResponse_descriptor,
        new java.lang.String[] { "IsAlive", });
    com.google.protobuf.EmptyProto.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
