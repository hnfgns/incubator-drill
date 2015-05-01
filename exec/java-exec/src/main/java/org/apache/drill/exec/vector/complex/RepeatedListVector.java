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
package org.apache.drill.exec.vector.complex;

import com.google.common.base.Preconditions;
import io.netty.buffer.DrillBuf;

import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.ComplexHolder;
import org.apache.drill.exec.expr.holders.RepeatedListHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.vector.AddOrGetResult;
import org.apache.drill.exec.vector.BaseRepeatedValueVector;
import org.apache.drill.exec.vector.RepeatedFixedWidthVectorLike;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.vector.RepeatedValueVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VectorDescriptor;
import org.apache.drill.exec.vector.complex.impl.NullReader;
import org.apache.drill.exec.vector.complex.impl.RepeatedListReaderImpl;
import org.apache.drill.exec.vector.complex.reader.FieldReader;


public class RepeatedListVector extends AbstractContainerVector
    implements RepeatedValueVector, RepeatedFixedWidthVectorLike {

  public final static MajorType TYPE = Types.repeated(MinorType.LIST);
  private final RepeatedListReaderImpl reader = new RepeatedListReaderImpl(null, this);
  private final DelegateRepeatedVector delegate;

  protected static class DelegateRepeatedVector
      extends BaseRepeatedValueVector<DelegateRepeatedVector.RepeatedListAccessor, DelegateRepeatedVector.RepeatedListMutator> {

    private final RepeatedListAccessor accessor = new RepeatedListAccessor();
    private final RepeatedListMutator mutator = new RepeatedListMutator();
    private final EmptyValuePopulator emptyPopulator;
    private transient DelegateTransferPair ephPair;

    public class RepeatedListAccessor extends BaseRepeatedValueVector.BaseRepeatedAccessor {

      @Override
      public Object getObject(int index) {
        List<Object> list = new JsonStringArrayList();
        final int start = offsets.getAccessor().get(index);
        final int until = offsets.getAccessor().get(index+1);
        for (int i = start; i < until; i++) {
          list.add(vector.getAccessor().getObject(i));
        }
        return list;
      }

      public void get(int index, RepeatedListHolder holder) {
        assert index <= getValueCapacity();
        holder.start = getOffsetVector().getAccessor().get(index);
        holder.end = getOffsetVector().getAccessor().get(index+1);
      }

      public void get(int index, ComplexHolder holder) {
        final FieldReader reader = getReader();
        reader.setPosition(index);
        holder.reader = reader;
      }

      public void get(int index, int arrayIndex, ComplexHolder holder) {
        final RepeatedListHolder listHolder = new RepeatedListHolder();
        get(index, listHolder);
        int offset = listHolder.start + arrayIndex;
        if (offset >= listHolder.end) {
          holder.reader = NullReader.INSTANCE;
        } else {
          FieldReader r = getDataVector().getReader();
          r.setPosition(offset);
          holder.reader = r;
        }
      }
    }

    public class RepeatedListMutator extends BaseRepeatedValueVector.BaseRepeatedMutator {

      public int add(int index) {
        final int curEnd = getOffsetVector().getAccessor().get(index+1);
        getOffsetVector().getMutator().setSafe(index + 1, curEnd + 1);
        return curEnd;
      }

      @Override
      public void startNewValue(int index) {
        emptyPopulator.populate(index+1);
        super.startNewValue(index);
      }

      @Override
      public void setValueCount(int valueCount) {
        emptyPopulator.populate(valueCount);
        super.setValueCount(valueCount);
      }
    }


    public class DelegateTransferPair implements TransferPair {
      private final DelegateRepeatedVector target;
      private final TransferPair[] children;

      public DelegateTransferPair(DelegateRepeatedVector target) {
        this.target = Preconditions.checkNotNull(target);
        if (target.getDataVector() == DEFAULT_DATA_VECTOR) {
          target.addOrGetVector(VectorDescriptor.create(getDataVector().getField().getType()));
          target.getDataVector().allocateNew();
        }
        this.children = new TransferPair[] {
            getOffsetVector().makeTransferPair(target.getOffsetVector()),
            getDataVector().makeTransferPair(target.getDataVector())
        };
      }

      @Override
      public void transfer() {
        for (TransferPair child:children) {
          child.transfer();
        }
      }

      @Override
      public ValueVector getTo() {
        return target;
      }

      @Override
      public void splitAndTransfer(int startIndex, int length) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void copyValueSafe(int srcIndex, int destIndex) {
        final RepeatedListHolder holder = new RepeatedListHolder();
        getAccessor().get(srcIndex, holder);
        target.emptyPopulator.populate(destIndex+1);
        final TransferPair vectorTransfer = children[1];
        int newIndex = target.getOffsetVector().getAccessor().get(destIndex);
        //todo: make this a bulk copy.
        for (int i = holder.start; i < holder.end; i++, newIndex++) {
          vectorTransfer.copyValueSafe(i, newIndex);
        }
        target.getOffsetVector().getMutator().setSafe(destIndex + 1, newIndex);
      }
    }

    public DelegateRepeatedVector(SchemaPath path, BufferAllocator allocator) {
      this(MaterializedField.create(path, TYPE), allocator);
    }

    public DelegateRepeatedVector(MaterializedField field, BufferAllocator allocator) {
      super(field, allocator);
      this.emptyPopulator = new EmptyValuePopulator(getOffsetVector());
    }

    @Override
    public void allocateNew() throws OutOfMemoryRuntimeException {
      if (!allocateNewSafe()) {
        throw new OutOfMemoryRuntimeException();
      }
    }

    @Override
    protected SerializedField.Builder getMetadataBuilder() {
      return super.getMetadataBuilder();
    }

    @Override
    public TransferPair getTransferPair(FieldReference ref) {
      return makeTransferPair(new DelegateRepeatedVector(ref, allocator));
    }

    @Override
    public TransferPair makeTransferPair(ValueVector target) {
      return new DelegateTransferPair(DelegateRepeatedVector.class.cast(target));
    }

    @Override
    public RepeatedListAccessor getAccessor() {
      return accessor;
    }

    @Override
    public RepeatedListMutator getMutator() {
      return mutator;
    }

    @Override
    public FieldReader getReader() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void load(SerializedField metadata, DrillBuf buffer) {
      //TODO: get rid of group count completely
      final int valueCount = metadata.getGroupCount();
      final int bufOffset = offsets.load(valueCount + 1, buffer);
      final SerializedField childField = metadata.getChildList().get(0);
      if (getDataVector() == DEFAULT_DATA_VECTOR) {
        addOrGetVector(VectorDescriptor.create(childField.getMajorType()));
      }

      if (childField.getValueCount() == 0) {
        vector.clear();
      } else {
        vector.load(childField, buffer.slice(bufOffset, childField.getBufferLength()));
      }
    }

    public void copyFromSafe(int fromIndex, int thisIndex, DelegateRepeatedVector from) {
      if(ephPair == null || ephPair.target != from) {
        ephPair = DelegateTransferPair.class.cast(from.makeTransferPair(this));
      }
      ephPair.copyValueSafe(fromIndex, thisIndex);
    }

  }

  protected class RepeatedListTransferPair implements TransferPair {
    private final TransferPair delegate;

    public RepeatedListTransferPair(TransferPair delegate) {
      this.delegate = delegate;
    }

    public void transfer() {
      delegate.transfer();
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      delegate.splitAndTransfer(startIndex, length);
    }

    @Override
    public ValueVector getTo() {
      final DelegateRepeatedVector delegateVector = DelegateRepeatedVector.class.cast(delegate.getTo());
      return new RepeatedListVector(getField(), allocator, callBack, delegateVector);
    }

    @Override
    public void copyValueSafe(int from, int to) {
      delegate.copyValueSafe(from, to);
    }
  }

  public RepeatedListVector(SchemaPath path, BufferAllocator allocator, CallBack callBack) {
    this(MaterializedField.create(path, TYPE), allocator, callBack);
  }

  public RepeatedListVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    this(field, allocator, callBack, new DelegateRepeatedVector(field, allocator));
  }

  protected RepeatedListVector(MaterializedField field, BufferAllocator allocator, CallBack callBack, DelegateRepeatedVector delegate) {
    super(field, allocator, callBack);
    int childrenSize = field.getChildren().size();

    // repeated list vector should not have more than one child
    assert childrenSize <= 1;
    this.delegate = Preconditions.checkNotNull(delegate);
    if (childrenSize > 0) {
      MaterializedField child = field.getChildren().iterator().next();
      addOrGetVector(VectorDescriptor.create(child.getType()));
//      setVector(TypeHelper.getNewVector(child, allocator, callBack));
    }
  }


    @Override
  public RepeatedListReaderImpl getReader() {
    return reader;
  }

  @Override
  public DelegateRepeatedVector.RepeatedListAccessor getAccessor() {
    return delegate.getAccessor();
  }

  @Override
  public DelegateRepeatedVector.RepeatedListMutator getMutator() {
    return delegate.getMutator();
  }

  @Override
  public UInt4Vector getOffsetVector() {
    return delegate.getOffsetVector();
  }

  @Override
  public ValueVector getDataVector() {
    return delegate.getDataVector();
  }

  @Override
  public void allocateNew() throws OutOfMemoryRuntimeException {
    delegate.allocateNew();
  }

  @Override
  public boolean allocateNewSafe() {
    return delegate.allocateNewSafe();
  }

  @Override
  public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(VectorDescriptor descriptor) {
    return delegate.addOrGetVector(descriptor);
  }

  @Override
  public int size() {
    return delegate.size();
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
  public TransferPair getTransferPair() {
    return new RepeatedListTransferPair(delegate.getTransferPair());
  }

  @Override
  public TransferPair getTransferPair(FieldReference ref) {
    return new RepeatedListTransferPair(delegate.getTransferPair(ref));
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    final RepeatedListVector target = RepeatedListVector.class.cast(to);
    return new RepeatedListTransferPair(delegate.makeTransferPair(target.delegate));
  }

  @Override
  public int getValueCapacity() {
    return delegate.getValueCapacity();
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    return delegate.getBuffers(clear);
  }


  @Override
  public void load(SerializedField metadata, DrillBuf buf) {
    delegate.load(metadata, buf);
  }

  @Override
  public SerializedField getMetadata() {
    return delegate.getMetadata();
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return delegate.iterator();
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    delegate.setInitialCapacity(numRecords);
  }

  /**
   * @deprecated
   *   prefer using {@link #addOrGetVector(org.apache.drill.exec.vector.VectorDescriptor)} instead.
   */
  @Override
  public <T extends ValueVector> T addOrGet(String name, MajorType type, Class<T> clazz) {
    final AddOrGetResult<T> result = addOrGetVector(VectorDescriptor.create(type));
    if (result.isCreated() && callBack != null) {
      callBack.doWork();
    }
    return result.getVector();
  }

  @Override
  public <T extends ValueVector> T getChild(String name, Class<T> clazz) {
    if (name != null) {
      return null;
    }
    return typeify(delegate.getDataVector(), clazz);
  }

  @Override
  public void allocateNew(int valueCount, int innerValueCount) {
    clear();
    getOffsetVector().allocateNew(valueCount + 1);
    getMutator().reset();
  }

  @Override
  public int load(int valueCount, int innerValueCount, DrillBuf buf) {
    throw new UnsupportedOperationException();
  }

  @Override
  public VectorWithOrdinal getChildVectorWithOrdinal(String name) {
    if (name != null) {
      return null;
    }
    return new VectorWithOrdinal(delegate.getDataVector(), 0);
  }


  public void copyFromSafe(int fromIndex, int thisIndex, RepeatedListVector from) {
    delegate.copyFromSafe(fromIndex, thisIndex, from.delegate);
  }


//  protected void setVector(ValueVector newVector) {
//    vector = Preconditions.checkNotNull(newVector);
//    getField().addChild(newVector.getField());
//  }

}
