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

import java.nio.ByteBuffer;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.vector.ValueVector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestRecordBatchLoader {

  private static final int CAPACITY = 100000;
  private final TypeProtos.MajorType typeOne = Types.required(TypeProtos.MinorType.INT);
  private final TypeProtos.MajorType typeTwo = Types.required(TypeProtos.MinorType.BIT);
  private final UserBitShared.NamePart nameOne = UserBitShared.NamePart.newBuilder().setName("nameOne").build();
  private final UserBitShared.NamePart nameTwo = UserBitShared.NamePart.newBuilder().setName("nameTwo").build();

  @Mock
  private BufferAllocator allocator;
  private RecordBatchLoader loader;
  private DrillBuf incoming;

  @Before
  public void setUp() {
    Mockito.when(allocator.buffer(Mockito.anyInt())).thenReturn(newBuffer(CAPACITY, allocator));
    Mockito.when(allocator.getEmpty()).thenReturn(newBuffer(0, allocator));
    loader = new RecordBatchLoader(allocator);
    incoming = newBuffer(CAPACITY, allocator);
  }

  protected DrillBuf newBuffer(int capacity, BufferAllocator allocator) {
    return DrillBuf.wrapByteBuffer(ByteBuffer.allocateDirect(capacity), allocator);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testBufferLengthCannotBeSmallerThanRequired() throws Exception {
    final UserBitShared.SerializedField field = UserBitShared.SerializedField.newBuilder()
        .setMajorType(typeOne)
        .setNamePart(nameOne)
        .setBufferLength(0)
        .setValueCount(1)
        .build();

    final UserBitShared.RecordBatchDef def = UserBitShared.RecordBatchDef.newBuilder()
        .addField(field)
        .build();

    loader.load(def, incoming);
  }

  @Test(expected = IllegalStateException.class)
  public void testBufferMustBeConsumerEntirely() throws Exception {
    final UserBitShared.SerializedField field = UserBitShared.SerializedField.newBuilder()
        .setMajorType(typeOne)
        .setNamePart(nameOne)
        .setBufferLength(4)
        .setValueCount(1)
        .build();

    final UserBitShared.RecordBatchDef def = UserBitShared.RecordBatchDef.newBuilder()
        .addField(field)
        .build();

    // buffer has more than needed
    incoming.capacity(10);
    loader.load(def, incoming);
  }

  @Test
  public void testSingleVectorLoading() throws Exception {
    final UserBitShared.SerializedField field = UserBitShared.SerializedField.newBuilder()
        .setMajorType(typeOne)
        .setNamePart(nameOne)
        .setBufferLength(4)
        .setValueCount(1)
        .build();

    final UserBitShared.RecordBatchDef def = UserBitShared.RecordBatchDef.newBuilder()
        .addField(field)
        .setRecordCount(1)
        .build();

    incoming.capacity(4);

    final boolean schemaChanged = loader.load(def, incoming);
    Assert.assertTrue("schema must be changed", schemaChanged);
    Assert.assertEquals("field count must match", 1, loader.getSchema().getFieldCount());
    Assert.assertEquals("field must match", MaterializedField.create(field), loader.getSchema().getColumn(0));
    Assert.assertEquals("record count must match", 1, loader.getRecordCount());
    for (VectorWrapper wrapper:loader) {
      final ValueVector vector = wrapper.getValueVector();
      Assert.assertEquals(String.format("value count [%s] must match record count", vector.getField()),
          loader.getRecordCount(), vector.getAccessor().getValueCount());
    }
  }

  @Test
  public void testMultipleBatches() throws Exception {
    testSingleVectorLoading();

    final UserBitShared.SerializedField fieldTwo = UserBitShared.SerializedField.newBuilder()
        .setMajorType(typeTwo)
        .setNamePart(nameTwo)
        .setBufferLength(1)
        .setValueCount(2)
        .build();

    final UserBitShared.RecordBatchDef def = UserBitShared.RecordBatchDef.newBuilder()
        .addField(fieldTwo)
        .setRecordCount(2)
        .build();

    final DrillBuf incoming = newBuffer(1, allocator);
    final boolean schemaChanged = loader.load(def, incoming);

    Assert.assertTrue("schema must be changed", schemaChanged);
    Assert.assertEquals("field count must match", 1, loader.getSchema().getFieldCount());
    Assert.assertEquals("field must match", MaterializedField.create(fieldTwo), loader.getSchema().getColumn(0));
    Assert.assertEquals("record count must match", 2, loader.getRecordCount());
    for (VectorWrapper wrapper:loader) {
      final ValueVector vector = wrapper.getValueVector();
      Assert.assertEquals(String.format("value count [%s] must match record count", vector.getField()),
          loader.getRecordCount(), vector.getAccessor().getValueCount());
    }
  }
}
