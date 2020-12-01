package com.alibaba.tailbase.proto;

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
    comments = "Source: Server.proto")
public class BackendServiceGrpc {

  private BackendServiceGrpc() {}

  public static final String SERVICE_NAME = "server.BackendService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<setWrongTraceIdRequest,
      setWrongTraceIdReply> METHOD_SET_WRONG_TRACE_ID =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "server.BackendService", "setWrongTraceId"),
          io.grpc.protobuf.ProtoUtils.marshaller(setWrongTraceIdRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(setWrongTraceIdReply.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static BackendServiceStub newStub(io.grpc.Channel channel) {
    return new BackendServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static BackendServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new BackendServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static BackendServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new BackendServiceFutureStub(channel);
  }

  /**
   */
  @Deprecated public static interface BackendService {

    /**
     */
    public void setWrongTraceId(setWrongTraceIdRequest request,
                                io.grpc.stub.StreamObserver<setWrongTraceIdReply> responseObserver);
  }

  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1469")
  public static abstract class BackendServiceImplBase implements BackendService, io.grpc.BindableService {

    @Override
    public void setWrongTraceId(setWrongTraceIdRequest request,
                                io.grpc.stub.StreamObserver<setWrongTraceIdReply> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SET_WRONG_TRACE_ID, responseObserver);
    }

    @Override public io.grpc.ServerServiceDefinition bindService() {
      return BackendServiceGrpc.bindService(this);
    }
  }

  /**
   */
  @Deprecated public static interface BackendServiceBlockingClient {

    /**
     */
    public setWrongTraceIdReply setWrongTraceId(setWrongTraceIdRequest request);
  }

  /**
   */
  @Deprecated public static interface BackendServiceFutureClient {

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<setWrongTraceIdReply> setWrongTraceId(
            setWrongTraceIdRequest request);
  }

  public static class BackendServiceStub extends io.grpc.stub.AbstractStub<BackendServiceStub>
      implements BackendService {
    private BackendServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BackendServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected BackendServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BackendServiceStub(channel, callOptions);
    }

    @Override
    public void setWrongTraceId(setWrongTraceIdRequest request,
                                io.grpc.stub.StreamObserver<setWrongTraceIdReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_SET_WRONG_TRACE_ID, getCallOptions()), request, responseObserver);
    }
  }

  public static class BackendServiceBlockingStub extends io.grpc.stub.AbstractStub<BackendServiceBlockingStub>
      implements BackendServiceBlockingClient {
    private BackendServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BackendServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected BackendServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BackendServiceBlockingStub(channel, callOptions);
    }

    @Override
    public setWrongTraceIdReply setWrongTraceId(setWrongTraceIdRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_SET_WRONG_TRACE_ID, getCallOptions(), request);
    }
  }

  public static class BackendServiceFutureStub extends io.grpc.stub.AbstractStub<BackendServiceFutureStub>
      implements BackendServiceFutureClient {
    private BackendServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private BackendServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected BackendServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new BackendServiceFutureStub(channel, callOptions);
    }

    @Override
    public com.google.common.util.concurrent.ListenableFuture<setWrongTraceIdReply> setWrongTraceId(
        setWrongTraceIdRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_SET_WRONG_TRACE_ID, getCallOptions()), request);
    }
  }

  @Deprecated public static abstract class AbstractBackendService extends BackendServiceImplBase {}

  private static final int METHODID_SET_WRONG_TRACE_ID = 0;

  private static class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final BackendService serviceImpl;
    private final int methodId;

    public MethodHandlers(BackendService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SET_WRONG_TRACE_ID:
          serviceImpl.setWrongTraceId((setWrongTraceIdRequest) request,
              (io.grpc.stub.StreamObserver<setWrongTraceIdReply>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
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
        METHOD_SET_WRONG_TRACE_ID);
  }

  @Deprecated public static io.grpc.ServerServiceDefinition bindService(
      final BackendService serviceImpl) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          METHOD_SET_WRONG_TRACE_ID,
          asyncUnaryCall(
            new MethodHandlers<
              setWrongTraceIdRequest,
              setWrongTraceIdReply>(
                serviceImpl, METHODID_SET_WRONG_TRACE_ID)))
        .build();
  }
}
