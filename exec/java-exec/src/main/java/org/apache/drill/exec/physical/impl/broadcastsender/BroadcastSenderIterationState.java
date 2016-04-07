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
package org.apache.drill.exec.physical.impl.broadcastsender;

import org.apache.drill.exec.physical.impl.BaseRootExec;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.WritableBatch;

public class BroadcastSenderIterationState extends BaseRootExec.IterationState {
  public final int tunnelIndex;
  public final WritableBatch batch;

  public BroadcastSenderIterationState(final RecordBatch.IterOutcome outcome, final int tunnelIndex, final WritableBatch batch) {
    super(outcome);
    this.tunnelIndex = tunnelIndex;
    this.batch = batch;
  }
}