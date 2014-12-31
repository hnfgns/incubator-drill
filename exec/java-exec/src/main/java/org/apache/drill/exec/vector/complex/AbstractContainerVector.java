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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.collections.MapWithOrdinal;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.vector.ValueVector;

public abstract class AbstractContainerVector implements ValueVector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractContainerVector.class);

  private final MapWithOrdinal<String, ValueVector> vectors =  new MapWithOrdinal<>();
  private final MaterializedField field;
  protected final BufferAllocator allocator;
  protected final CallBack callBack;

  protected AbstractContainerVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    this.field = Preconditions.checkNotNull(field);
    this.allocator = allocator;
    this.callBack = callBack;
  }

  @Override
  public void allocateNew() throws OutOfMemoryRuntimeException {
    if (!allocateNewSafe()) {
      throw new OutOfMemoryRuntimeException();
    }
  }

  @Override
  public boolean allocateNewSafe() {
    for (ValueVector v : vectors.values()) {
      if (!v.allocateNewSafe()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public MaterializedField getField() {
    return field;
  }

  public <T extends ValueVector> T addOrGet(String name, MajorType type, Class<T> clazz) {
    boolean replace = false;
    for (int i=0; i<2; i++) {
      ValueVector vector = getVector(name);
      if (replace || vector == null) {
        vector = TypeHelper.getNewVector(field.getPath(), name, allocator, type);
        Preconditions.checkNotNull(vector, String.format("Failure to create vector of type %s.", type));
        putVector(name, vector);
        if (callBack != null) {
          callBack.doWork();
        }
      }
      if (clazz.isAssignableFrom(vector.getClass())) {
        return (T)vector;
      }
      boolean allNulls = true;
      for (int r=0; r<vector.getAccessor().getValueCount(); r++) {
        if (!vector.getAccessor().isNull(r)) {
          allNulls = false;
          break;
        }
      }
      if (!allNulls) {
        throw new IllegalStateException(String.format("Vector requested [%s] was different than type stored [%s].  Drill doesn't yet support hetergenous types.", clazz.getSimpleName(), vector.getClass().getSimpleName()));
      }
      vector.clear();
      replace = true;
    }
    throw new IllegalStateException(String.format("Unable to create vector[%s] of desired type[%s -- %s].", name, type, clazz.getSimpleName()));
  }

  public ValueVector getVectorByOrdinal(int id) {
    return vectors.getByOrdinal(id);
  }

  public ValueVector getVector(String name) {
    return getVector(name, ValueVector.class);
  }

  public <T extends ValueVector> T getVector(String name, Class<T> clazz) {
    ValueVector v = vectors.get(name.toLowerCase());
    if (v == null) {
      return null;
    }
    return typeify(v, clazz);
  }

  protected void putVector(String name, ValueVector vector) {
    ValueVector old = vectors.put(name.toLowerCase(), vector);
    if (old != null && old != vector) {
      logger.debug("Field [%s] mutated from [%s] to [%s]", name, old.getClass().getSimpleName(),
          vector.getClass().getSimpleName());
    }

    field.addChild(vector.getField());
  }

  protected Collection<String> getChildFieldNames() {
    return Sets.newLinkedHashSet(Iterables.transform(field.getChildren(), new Function<MaterializedField, String>() {
      @Nullable
      @Override
      public String apply(MaterializedField field) {
        return Preconditions.checkNotNull(field).getLastName();
      }
    }));
  }

  protected Collection<ValueVector> getVectors() {
    return vectors.values();
  }

  public int size() {
    return vectors.size();
  }

 @Override
  public void close() {
    for (ValueVector v : this) {
      v.close();
    }
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return vectors.values().iterator();
  }

  protected <T extends ValueVector> T typeify(ValueVector v, Class<T> clazz) {
    if (clazz.isAssignableFrom(v.getClass())) {
      return (T) v;
    }
    throw new IllegalStateException(String.format("Vector requested [%s] was different than type stored [%s].  Drill doesn't yet support hetergenous types.", clazz.getSimpleName(), v.getClass().getSimpleName()));
  }

  public List<ValueVector> getPrimitiveVectors() {
    List<ValueVector> primitiveVectors = Lists.newArrayList();
    for (ValueVector v : vectors.values()) {
      if (v instanceof AbstractContainerVector) {
        AbstractContainerVector av = (AbstractContainerVector) v;
        primitiveVectors.addAll(av.getPrimitiveVectors());
      } else {
        primitiveVectors.add(v);
      }
    }
    return primitiveVectors;
  }

  public VectorWithOrdinal getVectorWithOrdinal(String name) {
    final int ordinal = vectors.getOrdinal(name.toLowerCase());
    if (ordinal < 0) {
      return null;
    }
    final ValueVector vector = vectors.getByOrdinal(ordinal);
    return new VectorWithOrdinal(vector, ordinal);
  }

  public TypedFieldId getFieldIdIfMatches(TypedFieldId.Builder builder, boolean addToBreadCrumb, PathSegment seg) {
    if (seg == null) {
      if (addToBreadCrumb) {
        builder.intermediateType(this.getField().getType());
      }
      return builder.finalType(this.getField().getType()).build();
    }

    if (seg.isArray()) {
      if (seg.isLastPath()) {
        builder //
          .withIndex() //
          .finalType(getLastPathType());

        // remainder starts with the 1st array segment in SchemaPath.
        // only set remainder when it's the only array segment.
        if (addToBreadCrumb) {
          addToBreadCrumb = false;
          builder.remainder(seg);
        }
        return builder.build();
      } else {
        if (addToBreadCrumb) {
          addToBreadCrumb = false;
          builder.remainder(seg);
        }
        // this is a complex array reference, which means it doesn't correspond directly to a vector by itself.
        seg = seg.getChild();
      }
    } else {
      // name segment.
    }

    VectorWithOrdinal vord = getVectorWithOrdinal(seg.isArray() ? null : seg.getNameSegment().getPath());
    if (vord == null) {
      return null;
    }

    ValueVector v = vord.vector;
    if (addToBreadCrumb) {
      builder.intermediateType(v.getField().getType());
      builder.addId(vord.ordinal);
    }

    if (v instanceof AbstractContainerVector) {
      // we're looking for a multi path.
      AbstractContainerVector c = (AbstractContainerVector) v;
      return c.getFieldIdIfMatches(builder, addToBreadCrumb, seg.getChild());
    } else {
      if (seg.isNamed()) {
        if(addToBreadCrumb) {
          builder.intermediateType(v.getField().getType());
        }
        builder.finalType(v.getField().getType());
      } else {
        builder.finalType(v.getField().getType().toBuilder().setMode(DataMode.OPTIONAL).build());
      }

      if (seg.isLastPath()) {
        return builder.build();
      } else {
        PathSegment child = seg.getChild();
        if (child.isLastPath() && child.isArray()) {
          if (addToBreadCrumb) {
            builder.remainder(child);
          }
          builder.withIndex();
          builder.finalType(v.getField().getType().toBuilder().setMode(DataMode.OPTIONAL).build());
          return builder.build();
        } else {
          logger.warn("You tried to request a complex type inside a scalar object or path or type is wrong.");
          return null;
        }
      }
    }
  }


  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    List<DrillBuf> buffers = Lists.newArrayList();
    int expectedBufSize = getBufferSize();
    int actualBufSize = 0 ;

    for (ValueVector v : vectors.values()) {
      for (DrillBuf buf : v.getBuffers(clear)) {
        buffers.add(buf);
        actualBufSize += buf.writerIndex();
      }
    }

    Preconditions.checkArgument(actualBufSize == expectedBufSize);
    return buffers.toArray(new DrillBuf[buffers.size()]);
  }

  private MajorType getLastPathType() {
    if((this.getField().getType().getMinorType() == MinorType.LIST  &&
        this.getField().getType().getMode() == DataMode.REPEATED)) {  // Use Repeated scalar type instead of Required List.
      VectorWithOrdinal vord = getVectorWithOrdinal(null);
      ValueVector v = vord.vector;
      if (! (v instanceof  AbstractContainerVector)) {
        return v.getField().getType();
      }
    } else if (this.getField().getType().getMinorType() == MinorType.MAP  &&
        this.getField().getType().getMode() == DataMode.REPEATED) {  // Use Required Map
      return this.getField().getType().toBuilder().setMode(DataMode.REQUIRED).build();
    }

    return this.getField().getType();
  }

  protected boolean supportsDirectRead() {
    return false;
  }

}