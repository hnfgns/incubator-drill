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
package org.apache.drill.exec.record;

import io.netty.buffer.DrillBuf;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class RecordBatchLoader implements VectorAccessible, Iterable<VectorWrapper<?>>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RecordBatchLoader.class);

  private final VectorContainer container;
  private final BufferAllocator allocator;
  private int recordCount;

  protected RecordBatchLoader(VectorContainer container, BufferAllocator allocator) {
    this.container = Preconditions.checkNotNull(container, "container cannot be null");
    this.allocator = Preconditions.checkNotNull(allocator, "allocator cannot be null");
  }

  public RecordBatchLoader(BufferAllocator allocator) {
    this(new VectorContainer(), allocator);
  }

  /**
   * Load a record batch from a single incoming.
   *
   * @param def
   *          The definition for the record batch.
   * @param incoming
   *          The incoming that holds the data associated with the record batch
   * @return Whether or not the schema changed since the previous load.
   * @throws SchemaChangeException
   */
  public boolean load(RecordBatchDef def, DrillBuf incoming) throws SchemaChangeException {
    Preconditions.checkNotNull(def, "batch definition cannot be null");
    Preconditions.checkNotNull(incoming, "incoming buffer cannot be null");

    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    boolean schemaChanged = getSchema() == null;

    final Map<MaterializedField, ValueVector> oldFields = Maps.newHashMap();
    for (VectorWrapper<?> wrapper : container) {
      final ValueVector vector = wrapper.getValueVector();
      oldFields.put(vector.getField(), vector);
    }

    clear();

    final List<SerializedField> fields = def.getFieldList();
    int bufOffset = 0;
    for (SerializedField fmd : fields) {
      MaterializedField fieldDef = MaterializedField.create(fmd);
      ValueVector vector = oldFields.remove(fieldDef);

      if (vector == null) {
        schemaChanged = true;
        vector = TypeHelper.getNewVector(fieldDef, allocator);
      } else if (!vector.getField().getType().equals(fieldDef.getType())) {
        // clear previous vector
        vector.clear();
        schemaChanged = true;
        vector = TypeHelper.getNewVector(fieldDef, allocator);
      }

//      if (fmd.getValueCount() == 0 && (!fmd.hasGroupCount() || fmd.getGroupCount() == 0)) {
//        AllocationHelper.allocate(vector, 0, 0, 0);
//      } else {
      vector.load(fmd, incoming.slice(bufOffset, fmd.getBufferLength()));
//      }
      bufOffset += fmd.getBufferLength();
      container.add(vector);
    }

    Preconditions.checkState(bufOffset == incoming.capacity(), "received incoming must be consumed entirely");

    if (!oldFields.isEmpty()) {
      schemaChanged = true;
      for (ValueVector vector:oldFields.values()) {
        vector.close();
      }
    }

    recordCount = def.getRecordCount();
    container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return schemaChanged;
  }

  public TypedFieldId getValueVectorId(SchemaPath path) {
    return container.getValueVectorId(path);
  }

  public int getRecordCount() {
    return recordCount;
  }

  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids){
    return container.getValueAccessorById(clazz, ids);
  }

  public WritableBatch getWritableBatch() {
    boolean isSV2 = (getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE);
    return WritableBatch.getBatchNoHVWrap(recordCount, container, isSV2);
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return this.container.iterator();
  }

  public BatchSchema getSchema(){
    return container.getSchema();
  }

  public void clear(){
    container.clear();
    recordCount = 0;
  }

  public RecordBatchLoader canonicalize() {
    final VectorContainer canonical = VectorContainer.canonicalize(container);
    return new RecordBatchLoader(canonical, allocator);
  }
}
