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
package org.apache.drill.exec.work.fragment;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import org.apache.drill.common.CatastrophicFailure;
import org.apache.drill.common.DeferredException;
import org.apache.drill.common.SerializedExecutor;
import org.apache.drill.common.concurrent.ExtendedLatch;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.FragmentContext.ExecutorState;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.impl.ImplCreator;
import org.apache.drill.exec.physical.impl.IterationResult;
import org.apache.drill.exec.physical.impl.RootExec;
import org.apache.drill.exec.physical.impl.SendAvailabilityListener;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.FragmentState;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ControlsInjectorFactory;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.work.batch.IncomingBatchProvider;
import org.apache.drill.exec.work.batch.ReadAvailabilityListener;
import org.apache.drill.exec.work.foreman.DrillbitStatusListener;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Responsible for running a single fragment on a single Drillbit. Listens/responds to status request
 * and cancellation messages.
 */
public class FragmentExecutor implements Runnable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentExecutor.class);
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(FragmentExecutor.class);

  private final AtomicBoolean hasCloseoutThread = new AtomicBoolean(false);
  private final String fragmentName;
  private final FragmentContext fragmentContext;
  private final FragmentStatusReporter statusReporter;
  private final DeferredException deferredException = new DeferredException();
  private final PlanFragment fragment;
  private final FragmentRoot rootOperator;
  private final ReceiverExecutor receiverExecutor;

  private volatile RootExec root;
  private final AtomicReference<FragmentState> fragmentState = new AtomicReference<>(FragmentState.AWAITING_ALLOCATION);
  private final ExtendedLatch acceptExternalEvents = new ExtendedLatch();

  private DrillbitStatusListener drillbitStatusListener;
  private final FragmentHandle fragmentHandle;
  private final DrillbitContext drillbitContext;
  private final ClusterCoordinator clusterCoordinator;

  private final AtomicBoolean cleaned = new AtomicBoolean();
  private volatile boolean pendingCompletion; // true if last call to tryComplete was going to block

  /**
   * Create a FragmentExecutor where we need to parse and materialize the root operator.
   *
   * @param context
   * @param fragment
   * @param statusReporter
   */
  public FragmentExecutor(final FragmentContext context, final PlanFragment fragment,
      final FragmentStatusReporter statusReporter) {
    this(context, fragment, statusReporter, null);
  }

  /**
   * Create a FragmentExecutor where we already have a root operator in memory.
   *
   * @param context
   * @param fragment
   * @param statusReporter
   * @param rootOperator
   */
  public FragmentExecutor(final FragmentContext context, final PlanFragment fragment,
      final FragmentStatusReporter statusReporter, final FragmentRoot rootOperator) {
    this.fragmentContext = context;
    this.statusReporter = statusReporter;
    this.fragment = fragment;
    this.rootOperator = rootOperator;
    this.fragmentName = QueryIdHelper.getQueryIdentifier(context.getHandle());
    this.receiverExecutor = new ReceiverExecutor(fragmentName, fragmentContext.getExecutor());
    this.fragmentHandle = fragmentContext.getHandle();
    this.drillbitContext = fragmentContext.getDrillbitContext();
    this.clusterCoordinator = drillbitContext.getClusterCoordinator();
    context.setExecutorState(new ExecutorStateImpl());
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("FragmentExecutor [fragmentContext=");
    builder.append(fragmentContext);
    builder.append(", fragmentState=");
    builder.append(fragmentState);
    builder.append("]");
    return builder.toString();
  }

  /**
   * Returns the current fragment status if the fragment is running. Otherwise, returns no status.
   *
   * @return FragmentStatus or null.
   */
  public FragmentStatus getStatus() {
    /*
     * If the query is not in a running state, the operator tree is still being constructed and
     * there is no reason to poll for intermediate results.
     *
     * Previously the call to get the operator stats with the AbstractStatusReporter was happening
     * before this check. This caused a concurrent modification exception as the list of operator
     * stats is iterated over while collecting info, and added to while building the operator tree.
     */
    if (fragmentState.get() != FragmentState.RUNNING) {
      return null;
    }

    return statusReporter.getStatus(FragmentState.RUNNING);
  }

  /**
   * Cancel the execution of this fragment is in an appropriate state. Messages come from external.
   * NOTE that this will be called from threads *other* than the one running this runnable(),
   * so we need to be careful about the state transitions that can result.
   */
  public void cancel() {
    final boolean thisIsOnlyThread = hasCloseoutThread.compareAndSet(false, true);

    if (!thisIsOnlyThread) {
      acceptExternalEvents.awaitUninterruptibly();

      /*
       * We set the cancel requested flag but the actual cancellation is managed by the run() loop, if called.
       */
      updateState(FragmentState.CANCELLATION_REQUESTED);
    } else {
      // countdown so receiver fragment finished can proceed.
      acceptExternalEvents.countDown();

      updateState(FragmentState.CANCELLATION_REQUESTED);
      cleanup(FragmentState.FINISHED);
    }
  }

  private void cleanup(FragmentState state) {

    closeOutResources();

    updateState(state);
    // send the final state of the fragment. only the main execution thread can send the final state and it can
    // only be sent once.
    sendFinalState();

    if (drillbitStatusListener != null) {
      clusterCoordinator.removeDrillbitStatusListener(drillbitStatusListener);
    }
  }

  /**
   * Resume all the pauses within the current context. Note that this method will be called from threads *other* than
   * the one running this runnable(). Also, this method can be called multiple times.
   */
  public synchronized void unpause() {
    fragmentContext.getExecutionControls().unpauseAll();
  }

  /**
   * Inform this fragment that one of its downstream partners no longer needs additional records. This is most commonly
   * called in the case that a limit query is executed.
   *
   * @param handle The downstream FragmentHandle of the Fragment that needs no more records from this Fragment.
   */
  public void receivingFragmentFinished(final FragmentHandle handle) {
    receiverExecutor.submitReceiverFinished(handle);
  }

  static class FIFOTask implements Runnable, Comparable {
    private final static AtomicInteger sequencer = new AtomicInteger();
    private final Runnable delegate;
    private final FragmentHandle handle;
    private final int rank;

    public FIFOTask(final Runnable delegate, final FragmentHandle handle) {
      this.delegate = delegate;
      this.handle = handle;
      this.rank = sequencer.getAndIncrement();
    }

    @Override
    public void run() {
      delegate.run();
    }

    @Override
    public int compareTo(final Object o) {
      if (o instanceof FIFOTask) {
        final FIFOTask other = FIFOTask.class.cast(o);
        if (handle.getQueryId().equals(other.handle.getQueryId())) {
          final int result = handle.getMajorFragmentId() - other.handle.getMajorFragmentId();
          // break ties in fifo order
          if (result != 0) {
            return result;
          }
        }
        return rank - other.rank;
      }
      // otherwise arbitrary order
      return 0;
    }

    public static FIFOTask of(final Runnable delegate, final FragmentHandle handle) {
      return new FIFOTask(delegate, handle);
    }
  }

  @Override
  public void run() {
    // if a cancel thread has already entered this executor, we have not reason to continue.
    if (!hasCloseoutThread.compareAndSet(false, true)) {
      return;
    }

    boolean success = false;
    try {
      // if we didn't get the root operator when the executor was created, create it now.
      final FragmentRoot rootOperator = this.rootOperator != null ? this.rootOperator :
          drillbitContext.getPlanReader().readFragmentOperator(fragment.getFragmentJson());

      root = ImplCreator.getExec(fragmentContext, rootOperator);
      if (root == null) {
        logger.warn("unable to create RootExec for fragment {}", fragmentHandle);
        return;
      }

      drillbitStatusListener = new FragmentDrillbitStatusListener();
      clusterCoordinator.addDrillbitStatusListener(drillbitStatusListener);
      updateState(FragmentState.RUNNING);

      acceptExternalEvents.countDown();
      injector.injectPause(fragmentContext.getExecutionControls(), "fragment-running", logger);

      final DrillbitEndpoint endpoint = drillbitContext.getEndpoint();
      logger.debug("Starting fragment {}:{} on {}:{}",
          fragmentHandle.getMajorFragmentId(), fragmentHandle.getMinorFragmentId(),
          endpoint.getAddress(), endpoint.getUserPort());

      final UserGroupInformation queryUserUgi = fragmentContext.isImpersonationEnabled() ?
          ImpersonationUtil.createProxyUgi(fragmentContext.getQueryUserName()) :
          ImpersonationUtil.getProcessUserUGI();

      final Queue<Runnable> queue = fragmentContext.getDrillbitContext().getTaskQueue();

      queryUserUgi.doAs(new PrivilegedExceptionAction<Void>() {
        public Void run() throws Exception {
          final Runnable task = new Runnable() {
            int count = 1;

            @Override
            public void run() {
              final Thread executor = Thread.currentThread();
              final String originalThreadName = executor.getName();
              final String newThreadName = QueryIdHelper.getExecutorThreadName(fragmentHandle);
              /*
               * Run the query until root.next returns false OR we no longer need to continue.
               */
              executor.setName(newThreadName);

              boolean isCompleted;
              int iteration = 0;
              int maxIterations = Integer.MAX_VALUE;

              if (pendingCompletion) {
                pendingCompletion = false;
                logger.trace("resuming pending completion");
                boolean completed = tryComplete(); // finish cleaning up
                Preconditions.checkState(completed,
                  "tryComplete() shouldn't return false when resuming a pending completion");
                return;
              }

              try {
                do {
                  isCompleted = false;
                  iteration++;
                  try {
                    final IterationResult result = root.next();
                    logger.trace("this iteration resulted with {}", result);
                    switch (result) {
                      case SENDING_BUFFER_FULL:
                        final Runnable sendingTask = this;
                        root.setSendAvailabilityListener(new SendAvailabilityListener() {
                          @Override
                          public void onSendAvailable(final RootExec exec) {
                            queue.offer(FIFOTask.of(sendingTask, fragmentHandle));
                            logger.trace("sending provider is now available");
                          }
                        });
                        logger.trace("sending provider is full. backing off...");
                        return;
                      case NOT_YET:
                        final IncomingBatchProvider blockingProvider = Preconditions.checkNotNull(
                          fragmentContext.getAndResetBlockingIncomingBatchProvider(),
                          "blocking provider is required.");

                        final Runnable receivingTask = this;
                        blockingProvider.setReadAvailabilityListener(new ReadAvailabilityListener() {
                          @Override
                          public void onReadAvailable(final IncomingBatchProvider provider) {
                            queue.offer(FIFOTask.of(receivingTask, fragmentHandle));
                            logger.trace("reading provider is now available");
                          }
                        });
                        logger.trace("reading provider is empty. backing off...");
                        return;
                      case CONTINUE:
                        isCompleted = !shouldContinue();
                        final boolean lastIteration = iteration == maxIterations;
                        final boolean shouldDefer = !isCompleted && lastIteration;
                        logger.trace("executor state -> iterations: {} isCompleted? {} shouldDefer? {}", count++,
                          isCompleted, shouldDefer);
                        if (shouldDefer) {
                          queue.offer(this);
                          return;
                        }
                        break;
                      case COMPLETED:
                        isCompleted = true;
                        return;
                    }
                  } catch (final Exception ex) {
                    fail(ex);
                    isCompleted = true;
                  } finally {
                    if (isCompleted && !tryComplete()) {
                      pendingCompletion = true;
                      final Runnable completingTask = this;
                      fragmentContext.runWhenSendComplete(new FragmentContext.SendCompleteListener() {
                        @Override
                        public void sendComplete() {
                          if (cleaned.compareAndSet(false, true)) { // make sure this is only run once
                            queue.offer(FIFOTask.of(completingTask, fragmentHandle));
                            logger.trace("send completed, ready to resume completion of fragment {}",
                              QueryIdHelper.getQueryIdentifier(fragmentHandle));
                          }
                        }
                      });
                      logger.trace("waiting for send to complete. backing off...");
                    }
                  }
                } while (!isCompleted && iteration < maxIterations);
              } finally {
                executor.setName(originalThreadName);
              }
            }
          };

          injector.injectChecked(fragmentContext.getExecutionControls(), "fragment-execution", IOException.class);
          queue.offer(FIFOTask.of(task, fragmentHandle));

          return null;
        }
      });
      success = true;
    } catch (OutOfMemoryError | OutOfMemoryException e) {
      if (!(e instanceof OutOfMemoryError) || "Direct buffer memory".equals(e.getMessage())) {
        fail(UserException.memoryError(e).build(logger));
      } else {
        // we have a heap out of memory error. The JVM in unstable, exit.
        CatastrophicFailure.exit(e, "Unable to handle out of memory condition in FragmentExecutor.", -2);
      }
    } catch (AssertionError | Exception e) {
      fail(e);
    } finally {
      if (!success) {
        boolean completed = tryComplete();
        Preconditions.checkState(completed, "tryComplete shouldn't return false if the fragment failed starting");
      }
    }
  }

  private boolean tryComplete() {
    // We need to sure we countDown at least once. We'll do it here to guarantee that.
    acceptExternalEvents.countDown();

    if (!fragmentContext.isSendComplete()) {
      return false;
    }

    // here we could be in FAILED, RUNNING, or CANCELLATION_REQUESTED
    cleanup(FragmentState.FINISHED);

    return true;
  }

  /**
   * Utility method to check where we are in a no terminal state.
   *
   * @return Whether or not execution should continue.
   */
  private boolean shouldContinue() {
    return !isCompleted() && FragmentState.CANCELLATION_REQUESTED != fragmentState.get();
  }

  /**
   * Returns true if the fragment is in a terminal state
   *
   * @return Whether this state is in a terminal state.
   */
  public boolean isCompleted() {
    return isTerminal(fragmentState.get());
  }

  private void sendFinalState() {
    final FragmentState outcome = fragmentState.get();
    if (outcome == FragmentState.FAILED) {
      final FragmentHandle handle = getContext().getHandle();
      final UserException uex = UserException.systemError(deferredException.getAndClear())
          .addIdentity(getContext().getIdentity())
          .addContext("Fragment", handle.getMajorFragmentId() + ":" + handle.getMinorFragmentId())
          .build(logger);
      statusReporter.fail(uex);
    } else {
      statusReporter.stateChanged(outcome);
    }
  }


  private void closeOutResources() {

    // first close the operators and release all memory.
    try {
      // Say executor was cancelled before setup. Now when executor actually runs, root is not initialized, but this
      // method is called in finally. So root can be null.
      if (root != null) {
        root.close();
      }
    } catch (final Exception e) {
      fail(e);
    }

    // then close the fragment context.
    fragmentContext.close();

  }

  private void warnStateChange(final FragmentState current, final FragmentState target) {
    logger.warn(fragmentName + ": Ignoring unexpected state transition {} --> {}", current.name(), target.name());
  }

  private void errorStateChange(final FragmentState current, final FragmentState target) {
    final String msg = "%s: Invalid state transition %s --> %s";
    throw new StateTransitionException(String.format(msg, fragmentName, current.name(), target.name()));
  }

  private synchronized boolean updateState(FragmentState target) {
    final FragmentState current = fragmentState.get();
    logger.info(fragmentName + ": State change requested {} --> {}", current, target);
    switch (target) {
    case CANCELLATION_REQUESTED:
      switch (current) {
      case SENDING:
      case AWAITING_ALLOCATION:
      case RUNNING:
        fragmentState.set(target);
        statusReporter.stateChanged(target);
        return true;

      default:
        warnStateChange(current, target);
        return false;
      }

    case FINISHED:
      if(current == FragmentState.CANCELLATION_REQUESTED){
        target = FragmentState.CANCELLED;
      } else if (current == FragmentState.FAILED) {
        target = FragmentState.FAILED;
      }
      // fall-through
    case FAILED:
      if(!isTerminal(current)){
        fragmentState.set(target);
        // don't notify reporter until we finalize this terminal state.
        return true;
      } else if (current == FragmentState.FAILED) {
        // no warn since we can call fail multiple times.
        return false;
      } else if (current == FragmentState.CANCELLED && target == FragmentState.FAILED) {
        fragmentState.set(FragmentState.FAILED);
        return true;
      }else{
        warnStateChange(current, target);
        return false;
      }

    case RUNNING:
      if(current == FragmentState.AWAITING_ALLOCATION){
        fragmentState.set(target);
        statusReporter.stateChanged(target);
        return true;
      }else{
        errorStateChange(current, target);
      }

      // these should never be requested.
    case CANCELLED:
    case SENDING:
    case AWAITING_ALLOCATION:
    default:
      errorStateChange(current, target);
    }

    // errorStateChange() throw should mean this is never executed
    throw new IllegalStateException();
  }

  private boolean isTerminal(final FragmentState state) {
    return state == FragmentState.CANCELLED
        || state == FragmentState.FAILED
        || state == FragmentState.FINISHED;
  }

  /**
   * Capture an exception and add store it. Update state to failed status (if not already there). Does not immediately
   * report status back to Foreman. Only the original thread can return status to the Foreman.
   *
   * @param excep
   *          The failure that occurred.
   */
  private void fail(final Throwable excep) {
    logger.error("failure while running fragment", excep);
    deferredException.addThrowable(excep);
    updateState(FragmentState.FAILED);
  }

  public FragmentContext getContext() {
    return fragmentContext;
  }

  private class ExecutorStateImpl implements ExecutorState {
    public boolean shouldContinue() {
      return FragmentExecutor.this.shouldContinue();
    }

    public void fail(final Throwable t) {
      FragmentExecutor.this.fail(t);
    }

    public boolean isFailed() {
      return fragmentState.get() == FragmentState.FAILED;
    }
    public Throwable getFailureCause(){
      return deferredException.getException();
    }
  }

  private class FragmentDrillbitStatusListener implements DrillbitStatusListener {
    @Override
    public void drillbitRegistered(final Set<CoordinationProtos.DrillbitEndpoint> registeredDrillbits) {
    }

    @Override
    public void drillbitUnregistered(final Set<CoordinationProtos.DrillbitEndpoint> unregisteredDrillbits) {
      // if the defunct Drillbit was running our Foreman, then cancel the query
      final DrillbitEndpoint foremanEndpoint = FragmentExecutor.this.fragmentContext.getForemanEndpoint();
      if (unregisteredDrillbits.contains(foremanEndpoint)) {
        logger.warn("Foreman {} no longer active.  Cancelling fragment {}.",
                    foremanEndpoint.getAddress(),
                    QueryIdHelper.getQueryIdentifier(fragmentContext.getHandle()));
        FragmentExecutor.this.cancel();
      }
    }
  }

  private class ReceiverExecutor extends SerializedExecutor {

    public ReceiverExecutor(String name, Executor underlyingExecutor) {
      super(name, underlyingExecutor);
    }

    @Override
    protected void runException(Runnable command, Throwable t) {
      logger.error("Failure running with exception of command {}", command, t);
    }

    public void submitReceiverFinished(FragmentHandle handle){
      execute(new ReceiverFinished(handle));
    }
  }

  private class ReceiverFinished implements Runnable {
    final FragmentHandle handle;

    public ReceiverFinished(FragmentHandle handle) {
      super();
      this.handle = handle;
    }

    @Override
    public void run() {
      acceptExternalEvents.awaitUninterruptibly();

      if (root != null) {
        logger.info("Applying request for early sender termination for {} -> {}.",
            QueryIdHelper.getFragmentId(getContext().getHandle()), QueryIdHelper.getFragmentId(handle));
        root.receivingFragmentFinished(handle);
      } else {
        logger.warn("Dropping request for early fragment termination for path {} -> {} as no root exec exists.",
            QueryIdHelper.getFragmentId(getContext().getHandle()), QueryIdHelper.getFragmentId(handle));
      }

    }

  }

}
