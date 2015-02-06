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
package org.apache.drill.exec.vector;

import java.util.Iterator;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

public class EmptyValueVector implements ValueVector {
  private final ValueVector delegate;
  private final ValueVector.Accessor accessor = new Accessor() {
    @Override
    public Object getObject(int index) {
      return null;
    }

    @Override
    public int getValueCount() {
      return 0;
    }

    @Override
    public boolean isNull(int index) {
      return true;
    }

    @Override
    public void reset() { }

    @Override
    public FieldReader getReader() {
      throw new UnsupportedOperationException();
    }
  };

  public EmptyValueVector(ValueVector delegate) {
    this.delegate = delegate;
  }

  @Override
  public void allocateNew() throws OutOfMemoryRuntimeException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean allocateNewSafe() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBufferSize() {
    return delegate.getBufferSize();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  @Override
  public MaterializedField getField() {
    return delegate.getField();
  }

  @Override
  public TransferPair getTransferPair() {
    return delegate.getTransferPair();
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return delegate.makeTransferPair(to);
  }

  @Override
  public TransferPair getTransferPair(FieldReference ref) {
    return delegate.getTransferPair(ref);
  }

  @Override
  public int getValueCapacity() {
    return delegate.getValueCapacity();
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    return delegate.getBuffers(clear);
  }

  @Override
  public void load(UserBitShared.SerializedField metadata, DrillBuf buffer) {
    throw new UnsupportedOperationException("Empty value vector is immutable");
  }

  @Override
  public UserBitShared.SerializedField getMetadata() {
    return delegate.getMetadata();
  }

  @Override
  public Mutator getMutator() {
    throw new UnsupportedOperationException("Empty value vector is immutable");
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return delegate.iterator();
  }
}
