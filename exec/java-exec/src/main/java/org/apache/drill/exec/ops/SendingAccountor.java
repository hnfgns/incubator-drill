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
package org.apache.drill.exec.ops;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.drill.exec.ops.FragmentContext.SendCompleteListener;

/**
 * Account for whether all messages sent have been completed. Necessary before finishing a task so we don't think
 * buffers are hanging when they will be released.
 *
 * TODO: Need to update to use long for number of pending messages.
 *
 * Accepts a {@link SendCompleteListener} that will be called when all acks have been received.<br>
 * <pre>Few assumptions are made about this listener:
 * 1. the listener will only be set when the fragment is closing, so no more fragments will be sent and thus
 *    {@link #increment()} will not be called again
 * 2. the listener can be safely called more than once
 * </pre>
 */
class SendingAccountor {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SendingAccountor.class);

  private final AtomicInteger batchesSent = new AtomicInteger(0);
  private final Semaphore wait = new Semaphore(0);

  private volatile SendCompleteListener sendCompleteListener;

  void increment() {
    batchesSent.incrementAndGet();
  }

  void decrement() {
    wait.release();
    if (isSendComplete() && sendCompleteListener != null) {
        sendCompleteListener.sendComplete();
    }
  }

  /**
   * Adds a {@link SendCompleteListener} that will be called when we received ack for all outgoing batches
   * @param sendCompleteListener send complete listener
   */
  void setSendCompleteListener(SendCompleteListener sendCompleteListener) {
    this.sendCompleteListener = sendCompleteListener;
    if (isSendComplete()) {
      sendCompleteListener.sendComplete();
    }
  }

  /**
   * @return true, if we received ack for all outgoing batches
   */
  boolean isSendComplete() {
    return wait.availablePermits() == batchesSent.get();
  }
}
