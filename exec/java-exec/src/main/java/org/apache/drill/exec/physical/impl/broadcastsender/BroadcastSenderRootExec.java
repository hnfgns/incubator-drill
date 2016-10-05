/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.physical.impl.broadcastsender;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.AccountingDataTunnel;
import org.apache.drill.exec.ops.DrillbitDelegatingAccountingDataTunnel;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.physical.MinorFragmentEndpoint;
import org.apache.drill.exec.physical.config.BroadcastSender;
import org.apache.drill.exec.physical.impl.BaseRootExec;
import org.apache.drill.exec.physical.impl.IterationResult;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.WritableBatch;

/**
 * Broadcast Sender broadcasts incoming batches to all receivers (one or more).
 * This is useful in cases such as broadcast join where sending the entire table to join
 * to all nodes is cheaper than merging and computing all the joins in the same node.
 */
public class BroadcastSenderRootExec extends BaseRootExec<BroadcastSenderIterationState> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BroadcastSenderRootExec.class);
  private final BroadcastSender config;
  private final int[][] receivingMinorFragments;
  private final List<AccountingDataTunnel> tunnels;
  private final ExecProtos.FragmentHandle handle;
  private final RecordBatch incoming;

  public enum Metric implements MetricDef {
    N_RECEIVERS,
    BYTES_SENT;
    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public BroadcastSenderRootExec(final FragmentContext context, final RecordBatch incoming, final BroadcastSender config) {
    super(context, context.newOperatorContext(config, null), config);
    this.incoming = incoming;
    this.config = config;
    this.handle = context.getHandle();
    final List<MinorFragmentEndpoint> destinations = config.getDestinations();
    final ArrayListMultimap<DrillbitEndpoint, Integer> dests = ArrayListMultimap.create();

    for(MinorFragmentEndpoint destination : destinations) {
      dests.put(destination.getEndpoint(), destination.getId());
    }

    final int destCount = dests.keySet().size();
    int i = 0;

    this.tunnels = new ArrayList<>();
    this.receivingMinorFragments = new int[destCount][];
    for(final DrillbitEndpoint ep : dests.keySet()){
      List<Integer> minorsList = dests.get(ep);
      final int[] minorsArray = new int[minorsList.size()];
      int x = 0;
      for(Integer m : minorsList){
        minorsArray[x++] = m;
      }
      receivingMinorFragments[i] = minorsArray;
      tunnels.add(DrillbitDelegatingAccountingDataTunnel.of(context.getDataTunnel(ep),
          getSendAvailabilityNotifier()));
      i++;
    }
    assert tunnels.size() == i;
  }

  @Override
  protected boolean canSend() {
    for (final AccountingDataTunnel tunnel:tunnels) {
      if (!tunnel.isSendingBufferAvailable()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public IterationResult innerNext() {
    final BroadcastSenderIterationState state;
    final boolean isPendingIteration = hasPendingState();
    if (isPendingIteration) {
      state = restorePendingState();
      clearPendingState();
    } else {
      final RecordBatch.IterOutcome out = next(incoming);
      state = new BroadcastSenderIterationState(out, 0, null);
    }

    final RecordBatch.IterOutcome out = state.outcome;
    logger.debug("Outcome of sender next {}", out);
    switch(out){
      case NOT_YET:
        return IterationResult.NOT_YET;
      case OUT_OF_MEMORY:
        throw new OutOfMemoryException();
      case STOP:
      case NONE:
        for (int i = state.tunnelIndex; i < tunnels.size(); ++i) {
          if (!tunnels.get(i).isSendingBufferAvailable()) {
            savePendingState(new BroadcastSenderIterationState(out, i, null));
            return IterationResult.SENDING_BUFFER_FULL;
          }
          FragmentWritableBatch b2 = FragmentWritableBatch.getEmptyLast(
              handle.getQueryId(),
              handle.getMajorFragmentId(),
              handle.getMinorFragmentId(),
              config.getOppositeMajorFragmentId(),
              receivingMinorFragments[i]);
          stats.startWait();
          try {
            tunnels.get(i).sendRecordBatch(b2);
          } finally {
            stats.stopWait();
          }
        }
        return IterationResult.COMPLETED;

      case OK_NEW_SCHEMA:
      case OK:
        final WritableBatch writableBatch;
        if (isPendingIteration) {
          writableBatch = state.batch;
        } else {
          writableBatch = incoming.getWritableBatch().transfer(oContext.getAllocator());
          if (tunnels.size() > 1) {
            writableBatch.retainBuffers(tunnels.size() - 1);
          }
        }
        for (int i = state.tunnelIndex; i < tunnels.size(); ++i) {
          if (!tunnels.get(i).isSendingBufferAvailable()) {
            savePendingState(new BroadcastSenderIterationState(out, i, writableBatch));
            return IterationResult.SENDING_BUFFER_FULL;
          }
          FragmentWritableBatch batch = new FragmentWritableBatch(
              false,
              handle.getQueryId(),
              handle.getMajorFragmentId(),
              handle.getMinorFragmentId(),
              config.getOppositeMajorFragmentId(),
              receivingMinorFragments[i],
              writableBatch);
          stats.startWait();
          try {
            tunnels.get(i).sendRecordBatch(batch);
            updateStats(batch);
          } finally {
            stats.stopWait();
          }
        }
        return IterationResult.CONTINUE;
      default:
        throw new IllegalStateException();
    }
  }

  public void updateStats(FragmentWritableBatch writableBatch) {
    stats.setLongStat(Metric.N_RECEIVERS, tunnels.size());
    stats.addLongStat(Metric.BYTES_SENT, writableBatch.getByteCount());
  }
}
