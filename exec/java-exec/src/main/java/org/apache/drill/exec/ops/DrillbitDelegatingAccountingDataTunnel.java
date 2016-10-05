/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.ops;

import com.google.common.base.Preconditions;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.rpc.CompositeRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ExecutionControls;
import org.slf4j.Logger;

public class DrillbitDelegatingAccountingDataTunnel implements AccountingDataTunnel {
  private final DrillbitAccountingDataTunnel delegate;
  private final RpcOutcomeListener<GeneralRPCProtos.Ack> listener;

  public DrillbitDelegatingAccountingDataTunnel(final DrillbitAccountingDataTunnel delegate,
                                                final RpcOutcomeListener<GeneralRPCProtos.Ack> listener) {
    this.delegate = Preconditions.checkNotNull(delegate, "delegate is required");
    this.listener = CompositeRpcOutcomeListener.of(delegate.getStatusHandler(), listener);
  }

  @Override
  public boolean isSendingBufferAvailable() {
    return delegate.isSendingBufferAvailable();
  }

  @Override
  public RpcOutcomeListener<GeneralRPCProtos.Ack> getStatusHandler() {
    return listener;
  }

  @Override
  public void sendRecordBatch(FragmentWritableBatch batch) {
    sendRecordBatch(getStatusHandler(), batch);
  }

  @Override
  public void sendRecordBatch(RpcOutcomeListener<GeneralRPCProtos.Ack> listener, FragmentWritableBatch batch) {
    delegate.sendRecordBatch(listener, batch);
  }

  @Override
  public void setTestInjectionControls(ControlsInjector testInjector, ExecutionControls testControls, Logger testLogger) {
    delegate.setTestInjectionControls(testInjector, testControls, testLogger);
  }

  public static DrillbitDelegatingAccountingDataTunnel of(final DrillbitAccountingDataTunnel delegate,
                                                          final RpcOutcomeListener<GeneralRPCProtos.Ack> listener) {
    return new DrillbitDelegatingAccountingDataTunnel(delegate, listener);
  }
}
