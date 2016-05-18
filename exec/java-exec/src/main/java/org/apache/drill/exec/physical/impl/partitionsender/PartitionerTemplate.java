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
package org.apache.drill.exec.physical.impl.partitionsender;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import javax.inject.Named;

import com.google.common.base.Preconditions;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.compile.sig.RuntimeOverridden;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.AccountingDataTunnel;
import org.apache.drill.exec.ops.DelegatingAccountingDataTunnel;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.MinorFragmentEndpoint;
import org.apache.drill.exec.physical.config.HashPartitionSender;
import org.apache.drill.exec.physical.impl.partitionsender.PartitionSenderRootExec.Metric;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SimpleVectorWrapper;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.vector.CopyUtil;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Lists;

public abstract class PartitionerTemplate implements Partitioner {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionerTemplate.class);

  // Always keep the recordCount as (2^x) - 1 to better utilize the memory allocation in ValueVectors
  private static final int DEFAULT_RECORD_BATCH_SIZE = (1 << 10) - 1;

  private SelectionVector2 sv2;
  private SelectionVector4 sv4;
  private RecordBatch incoming;
  private OperatorStats stats;
  private int start;
  private int end;
  private List<OutgoingRecordBatch> outgoingBatches = Lists.newArrayList();

  private int outgoingRecordBatchSize = DEFAULT_RECORD_BATCH_SIZE;

  public PartitionerTemplate() { }

  @Override
  public List<? extends PartitionOutgoingBatch> getOutgoingBatches() {
    return outgoingBatches;
  }

  @Override
  public PartitionOutgoingBatch getOutgoingBatch(int index) {
    if ( index >= start && index < end) {
      return outgoingBatches.get(index - start);
    }
    return null;
  }

  @Override
  public void setup(FragmentContext context, RecordBatch incoming, HashPartitionSender popConfig,
                          OperatorStats stats, OperatorContext oContext, int start, int end,
                          final RpcOutcomeListener<GeneralRPCProtos.Ack> sendAvailabilityNotifier) {
    this.incoming = incoming;
    this.stats = stats;
    this.start = start;
    this.end = end;
    doSetup(context, incoming, null);

    // Half the outgoing record batch size if the number of senders exceeds 1000 to reduce the total amount of memory
    // allocated.
    if (popConfig.getDestinations().size() > 1000) {
      // Always keep the recordCount as (2^x) - 1 to better utilize the memory allocation in ValueVectors
      outgoingRecordBatchSize = (DEFAULT_RECORD_BATCH_SIZE + 1)/2 - 1;
    }

    int fieldId = 0;
    logger.debug("initializing outgoing batches. start: {}, end: {}", start, end);
    for (MinorFragmentEndpoint destination : popConfig.getDestinations()) {
      // create outgoingBatches only for subset of Destination Points
      if ( fieldId >= start && fieldId < end ) {
        final AccountingDataTunnel tunnel = DelegatingAccountingDataTunnel.of(context.getDataTunnel(destination), sendAvailabilityNotifier);
        outgoingBatches.add(new OutgoingRecordBatch(stats, popConfig, tunnel, context, oContext.getAllocator()));
      }
      fieldId++;
    }

    // do not initialize before receiving ok_new_schema
    if (incoming.getSchema() == null) {
      return;
    }

    SelectionVectorMode svMode = incoming.getSchema().getSelectionVectorMode();
    switch(svMode){
      case FOUR_BYTE:
        this.sv4 = incoming.getSelectionVector4();
        break;

      case TWO_BYTE:
        this.sv2 = incoming.getSelectionVector2();
        break;

      case NONE:
        break;

      default:
        throw new UnsupportedOperationException("Unknown selection vector mode: " + svMode.toString());
    }

    initialize();
  }

  @Override
  public int getRecordCount() {
    int count = 0;
    for (final OutgoingRecordBatch batch:outgoingBatches) {
      count += batch.getRecordCount();
    }
    return count;
  }

  @Override
  public List<Integer> getRecordCounts() {
    final List<Integer> counts = Lists.newArrayList();
    for (final OutgoingRecordBatch batch:outgoingBatches) {
      counts.add(batch.getRecordCount());
    }
    return counts;
  }

  protected void initialize() {
    for (final OutgoingRecordBatch batch : outgoingBatches) {
      // skip completed batches
      if (batch.isCompleted()) {
        continue;
      }

      batch.initialize();
    }
  }

  @Override
  public OperatorStats getStats() {
    return stats;
  }

  @Override
  public boolean flush(final boolean isLast, final boolean ignoreBatchLimits) {
    boolean allFlushed = true;
    for (final OutgoingRecordBatch batch : outgoingBatches) {
      // skip completed batches
      if (batch.isCompleted()) {
        continue;
      }

      final int numRecords = batch.recordCount;
      final boolean shouldFlush = isLast // if the last batch
          || (ignoreBatchLimits && numRecords > 0) // if batch is non-empty and we ignore about batch limits
          || (numRecords >= outgoingRecordBatchSize); // if batch is filled
      if (shouldFlush) {
        final boolean isFlushed = batch.flush(isLast);
        if (isFlushed && !isLast) {
          batch.initialize();
        }
        allFlushed = allFlushed && isFlushed;
      }
    }
    return allFlushed;
  }

  @Override
  public void partitionBatch(final RecordBatch incoming) {
    final SelectionVectorMode svMode = incoming.getSchema().getSelectionVectorMode();
    final int recordCount = incoming.getRecordCount();
    logger.trace("partitioning {} records", incoming.getRecordCount());

    // Keeping the for loop inside the case to avoid case evaluation for each record.
    switch(svMode) {
      case NONE:
        for (int recordId = 0; recordId < recordCount; ++recordId) {
          doCopy(recordId, recordCount);
        }
        break;

      case TWO_BYTE:
        for (int recordId = 0; recordId < recordCount; ++recordId) {
          int svIndex = sv2.getIndex(recordId);
          doCopy(svIndex, recordCount);
        }
        break;

      case FOUR_BYTE:
        for (int recordId = 0; recordId < recordCount; ++recordId) {
          int svIndex = sv4.get(recordId);
          doCopy(svIndex, recordCount);
        }
        break;

      default:
        throw new UnsupportedOperationException("Unknown selection vector mode: " + svMode.toString());
    }
  }

  /**
   * Helper method to copy data based on partition
   * @param svIndex
   * @throws IOException
   */
  private void doCopy(final int svIndex, final int incomingRecordCount) {
    int index = doEval(svIndex);
    if (index < start || index >= end) {
      throw new IndexOutOfBoundsException(String.format("Computed batch index[%d] is beyond max[%d]", index, end));
    }
    final OutgoingRecordBatch outgoingBatch = outgoingBatches.get(index - start);
    outgoingBatch.copy(svIndex);
    if (outgoingBatch.recordCount == outgoingRecordBatchSize) {
      outgoingBatch.reinitialize(outgoingBatch.recordCount + incomingRecordCount - svIndex);
    }
  }

  @Override
  public void clear() {
    for (OutgoingRecordBatch outgoingRecordBatch : outgoingBatches) {
      outgoingRecordBatch.clear();
    }
  }

  @Override
  public boolean canSend() {
    for (final OutgoingRecordBatch batch:outgoingBatches) {
      // skip completed batches
      if (batch.isCompleted()) {
        continue;
      }

      if (!batch.tunnel.isSendingBufferAvailable()) {
        return false;
      }
    }
    return true;
  }

  public abstract void doSetup(@Named("context") FragmentContext context, @Named("incoming") RecordBatch incoming, @Named("outgoing") OutgoingRecordBatch[] outgoing);
  public abstract int doEval(@Named("inIndex") int inIndex);

  public class OutgoingRecordBatch implements PartitionOutgoingBatch, VectorAccessible {

    private final AccountingDataTunnel tunnel;
    private final HashPartitionSender operator;
    private final FragmentContext context;
    private final BufferAllocator allocator;
    private final VectorContainer vectorContainer = new VectorContainer();
    private final OperatorStats stats;

    private volatile PartitionState state = PartitionState.INITIAL;
    private int recordCount;
    private int totalRecords;

    public OutgoingRecordBatch(OperatorStats stats, HashPartitionSender operator, AccountingDataTunnel tunnel,
                               FragmentContext context, BufferAllocator allocator) {
      this.context = context;
      this.allocator = allocator;
      this.operator = operator;
      this.tunnel = tunnel;
      this.stats = stats;
    }

    protected void copy(int inIndex) {
      doEval(inIndex, recordCount);
      recordCount++;
      totalRecords++;
    }

    @Override
    public void terminate() {
      // receiver already terminated, don't send anything to it from now on
      state = PartitionState.CANCELLED;
    }

    public boolean isCompleted() {
      return state == PartitionState.COMPLETED;
    }

    @Override
    public PartitionState getState() {
      return state;
    }

    @RuntimeOverridden
    protected void doSetup(@Named("incoming") RecordBatch incoming, @Named("outgoing") VectorAccessible outgoing) {};

    @RuntimeOverridden
    protected void doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex) { };

    public boolean send(final FragmentWritableBatch batch) {
      stats.startWait();
      try {
        if (!tunnel.isSendingBufferAvailable()) {
          return false;
        }
        tunnel.sendRecordBatch(batch);
        updateStats(batch);
      } finally {
        stats.stopWait();
      }
      return true;
    }

    public boolean flush(final boolean isLastBatch) {
      if (isCompleted()) {
        throw new IllegalStateException("illegal attempt to flush partition in completed state");
      }

      if (logger.isTraceEnabled()) {
        final String msg = isLastBatch ? "attemping to flush last batch to frag:{}:{} [recordCount={}; totalRecords={}]"
            : "attemping to flush outgoing batch to frag:{}:{} [recordCount={}; totalRecords={}]";

        logger.trace(msg, operator.getOppositeMajorFragmentId(), tunnel.getRemoteEndpoint().getId(), recordCount, totalRecords);
      }
      // first, we make sure tunnel is writable before creating writable batch since getWritableBatch cleans incoming
      // batch. there is no way to retry sending the batch once the incoming batch is cleared.
      if (!tunnel.isSendingBufferAvailable()) {
        if (isLastBatch) {
          logger.trace("send buffer full: flushing last batch to frag:{}:{}", operator.getOppositeMajorFragmentId(),
              tunnel.getRemoteEndpoint().getId());
        }
        return false;
      }

      final FragmentHandle handle = context.getHandle();
      final FragmentWritableBatch batch;
      if (state == PartitionState.CANCELLED) {
        // we drop all from this point on, always send last batch.
        batch = FragmentWritableBatch.getEmptyLastWithSchema(handle.getQueryId(),
            handle.getMajorFragmentId(),
            handle.getMinorFragmentId(),
            operator.getOppositeMajorFragmentId(),
            tunnel.getRemoteEndpoint().getId(),
            incoming.getSchema());

        logger.trace("state was cancelled: flushing last batch to frag:{}:{}", operator.getOppositeMajorFragmentId(),
            tunnel.getRemoteEndpoint().getId());

      } else {
        // if no records found send an empty batch with schema
        batch = new FragmentWritableBatch(isLastBatch,
            handle.getQueryId(),
            handle.getMajorFragmentId(),
            handle.getMinorFragmentId(),
            operator.getOppositeMajorFragmentId(),
            tunnel.getRemoteEndpoint().getId(), // opposite minor fragment id
            getWritableBatch());
      }

      if (isLastBatch || state == PartitionState.CANCELLED) {
        // mark this outgoing batch as completed so that no other batch is sent through.
        state = PartitionState.COMPLETED;
      }

      // we transferred outgoing buffers. tunnel is writable. time to clean up outgoing vector.
      clear();

      if (!send(batch)) {
        logger.error("this should never happen.");
        return false;
      }

      return true;
    }

    public void updateStats(FragmentWritableBatch writableBatch) {
      stats.addLongStat(Metric.BYTES_SENT, writableBatch.getByteCount());
      stats.addLongStat(Metric.BATCHES_SENT, 1);
      stats.addLongStat(Metric.RECORDS_SENT, writableBatch.getHeader().getDef().getRecordCount());
    }

    /**
     * Initialize the OutgoingBatch based on the current schema in incoming RecordBatch
     */
    public void initialize() {
      for (VectorWrapper<?> v : incoming) {
        // create new vector
        final ValueVector outgoingVector = TypeHelper.getNewVector(v.getField(), allocator);
        outgoingVector.setInitialCapacity(outgoingRecordBatchSize);
        outgoingVector.allocateNew();
        vectorContainer.add(outgoingVector);
        vectorContainer.buildSchema(SelectionVectorMode.NONE);
      }
      doSetup(incoming, vectorContainer);
    }

    @Override
    public BatchSchema getSchema() {
      return incoming.getSchema();
    }

    @Override
    public int getRecordCount() {
      return recordCount;
    }


    @Override
    public long getTotalRecords() {
      return totalRecords;
    }

    @Override
    public TypedFieldId getValueVectorId(SchemaPath path) {
      return vectorContainer.getValueVectorId(path);
    }

    @Override
    public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... fieldIds) {
      return vectorContainer.getValueAccessorById(clazz, fieldIds);
    }

    @Override
    public Iterator<VectorWrapper<?>> iterator() {
      return vectorContainer.iterator();
    }

    @Override
    public SelectionVector2 getSelectionVector2() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SelectionVector4 getSelectionVector4() {
      throw new UnsupportedOperationException();
    }

    public void reinitialize(final int expectedCapacity) {
      final List<VectorWrapper<?>> wrappers = Lists.newArrayList(vectorContainer);
      final List<ValueVector> newVectors = Lists.newArrayList();
      logger.warn("re-initializing with expected capacity of {}; recordCount={}; totalRecords={}",
          expectedCapacity,
          recordCount,
          totalRecords);
      logger.warn("schema before expansion {}", vectorContainer.getSchema());
      for (final VectorWrapper wrapper:wrappers) {
        final ValueVector oldVector = wrapper.getValueVector();

        if (CopyUtil.useCopyFromSafe(wrapper.getField().getType())) {
          TransferPair tp = oldVector.getTransferPair(allocator);
          tp.transfer();
          newVectors.add(tp.getTo());
          continue;
        }

        final ValueVector newVector = TypeHelper.getNewVector(oldVector.getField(), allocator);
        newVectors.add(newVector);

        // ensure new vector has capacity for demand
        newVector.setInitialCapacity(expectedCapacity);
        newVector.allocateNew();

        oldVector.getMutator().setValueCount(recordCount);
        newVector.getMutator().setValueCount(expectedCapacity);

        final DrillBuf[] oldBufs = oldVector.getBuffers(false);
        final DrillBuf[] newBufs = newVector.getBuffers(false);

        logger.warn("old buffers: {} at {}", oldBufs.length, oldVector.getField());
        Preconditions.checkState(oldBufs.length == newBufs.length, "new & old buffer length must match");

        for (int i=0; i<oldBufs.length;i++) {
          newBufs[i].setBytes(0, oldBufs[i], 0, oldBufs[i].capacity());
        }
      }

      // clear container with old vectors.
      vectorContainer.clear();

      // re-populate container with new vectors.
      for (final ValueVector newVector:newVectors) {
        vectorContainer.add(newVector);
      }
      vectorContainer.buildSchema(SelectionVectorMode.NONE);
      logger.warn("schema after expansion {}", vectorContainer.getSchema());

      doSetup(incoming, vectorContainer);
    }

    public void setContainerSize(final int capacity) {
      for (final VectorWrapper<?> vector : vectorContainer) {
        vector.getValueVector().getMutator().setValueCount(capacity);
      }
    }

    public WritableBatch getWritableBatch() {
      setContainerSize(recordCount);
      return WritableBatch.getBatchNoHVWrap(recordCount, this, false);
    }

    @Override
    public void clear() {
      recordCount = 0;
      vectorContainer.clear();
    }

    @Override
    public AccountingDataTunnel getTunnel() {
      return tunnel;
    }


  }
}
