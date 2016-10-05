/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl;

import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentDelegatingAccountingDataTunnel;
import org.apache.drill.exec.ops.FragmentAccountingDataTunnel;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.physical.MinorFragmentEndpoint;
import org.apache.drill.exec.physical.config.SingleSender;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ControlsInjectorFactory;

public class SingleSenderCreator implements RootCreator<SingleSender>{

  @Override
  public RootExec getRoot(FragmentContext context, SingleSender config, List<RecordBatch> children)
      throws ExecutionSetupException {
    assert children != null && children.size() == 1;
    return new SingleSenderRootExec(context, children.iterator().next(), config);
  }

  public static class SingleSenderRootExec extends BaseRootExec {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingleSenderRootExec.class);
    private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(SingleSenderRootExec.class);

    private final FragmentHandle oppositeHandle;

    private RecordBatch incoming;
    private FragmentAccountingDataTunnel tunnel;
    private FragmentHandle handle;
    private volatile boolean ok = true;
    private volatile boolean done = false;

    public enum Metric implements MetricDef {
      BYTES_SENT;

      @Override
      public int metricId() {
        return ordinal();
      }
    }

    public SingleSenderRootExec(FragmentContext context, RecordBatch batch, SingleSender config) throws OutOfMemoryException {
      super(context, context.newOperatorContext(config, null), config);
      this.incoming = batch;
      assert incoming != null;
      handle = context.getHandle();
      oppositeHandle = handle.toBuilder()
          .setMajorFragmentId(config.getOppositeMajorFragmentId())
          .setMinorFragmentId(config.getOppositeMinorFragmentId())
          .build();
      final MinorFragmentEndpoint endpoint = MinorFragmentEndpoint.of(config.getOppositeMinorFragmentId(),
          config.getDestination());
      tunnel = context.getDataTunnel(endpoint);
      tunnel = FragmentDelegatingAccountingDataTunnel.of(context.getDataTunnel(endpoint), getSendAvailabilityNotifier());
      tunnel.setTestInjectionControls(injector, context.getExecutionControls(), logger);
    }

    @Override
    protected boolean canSend() {
      return tunnel.isSendingBufferAvailable();
    }

    @Override
    public IterationResult innerNext() {
      if (!ok) {
        incoming.kill(false);
        return IterationResult.COMPLETED;
      }

      IterOutcome out;
      final boolean isPendingIteration = hasPendingState();
      if (isPendingIteration) {
        if (!canSend()) { // this should never happen
          logger.error("sending buffers must have been available at this point");
          return IterationResult.SENDING_BUFFER_FULL;
        }
        // restore previous outcome
        final IterationState pendingState = restorePendingState();
        clearPendingState();
        out = pendingState.outcome;
        logger.trace("restored pending state outcome {}", out);
      } else if (!done) {
        out = next(incoming);
        // if we got a state where we need to send a batch but buffer is not available. save the state and back off.
        logger.trace("got new outcome {}", out);
        if (RootExecHelper.isInSendingState(out) && !canSend()) {
          savePendingState(IterationState.of(out));
          logger.trace("cannot send. saving state of {}", out);
          return IterationResult.SENDING_BUFFER_FULL;
        }
      } else {
        incoming.kill(true);
        out = IterOutcome.NONE;
      }
      clearPendingState();
//      logger.debug("Outcome of sender next {}", out);
      switch (out) {
      case NOT_YET:
        return IterationResult.NOT_YET;
      case OUT_OF_MEMORY:
        throw new OutOfMemoryException();
      case STOP:
      case NONE:
        // if we didn't do anything yet, send an empty schema.
        final BatchSchema sendSchema = incoming.getSchema() == null ?
            BatchSchema.newBuilder().build() : incoming.getSchema();

        final FragmentWritableBatch b2 = FragmentWritableBatch.getEmptyLastWithSchema(handle.getQueryId(),
            handle.getMajorFragmentId(), handle.getMinorFragmentId(), oppositeHandle.getMajorFragmentId(), oppositeHandle.getMinorFragmentId(),
            sendSchema);
        stats.startWait();
        try {
          tunnel.sendRecordBatch(b2);
          updateStats(b2);
        } finally {
          stats.stopWait();
        }
        return IterationResult.COMPLETED;

      case OK_NEW_SCHEMA:
      case OK:
        final FragmentWritableBatch batch = new FragmentWritableBatch(
            false, handle.getQueryId(), handle.getMajorFragmentId(),
            handle.getMinorFragmentId(), oppositeHandle.getMajorFragmentId(), oppositeHandle.getMinorFragmentId(),
            incoming.getWritableBatch().transfer(oContext.getAllocator()));
        stats.startWait();
        try {
          tunnel.sendRecordBatch(batch);
          updateStats(batch);
        } finally {
          stats.stopWait();
        }
        return IterationResult.CONTINUE;
      default:
        throw new IllegalStateException();
      }
    }

    public void updateStats(FragmentWritableBatch writableBatch) {
      stats.addLongStat(Metric.BYTES_SENT, writableBatch.getByteCount());
    }

    @Override
    public void receivingFragmentFinished(FragmentHandle handle) {
      done = true;
    }
  }
}
