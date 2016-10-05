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
package org.apache.drill.exec.work.batch;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.junit.Test;

/**
 * Reproduction for regression in BaseRawBatchBuffer
 */
public class TestBaseRawBatchBuffer extends BaseTestQuery {

  private static final String TEST_RES_PATH = TestTools.getWorkingPath() + "/src/test/resources";

  @Test // regression in functional test limit0/aggregates/aggregation/scalar/data/q19.sql
  public void testQ19() throws Exception {
    setSessionOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY, "10");
    setSessionOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY, "true");
    setSessionOption(ExecConstants.SLICE_TARGET, "1");

    test("select 100, count(*) + 1000, count(distinct a.c_boolean) " +
      "from dfs_test.`%s/work/batch/alltypes` a, dfs_test.`%s/work/batch/alltypes` b " +
      "where a.c_timestamp = b.c_timestamp " +
      "having " +
      "( count(distinct b.c_date) > 50 or sum(distinct b.c_integer) > 0 ) " +
      "and sum(distinct a.c_float) >= 1000000.00", TEST_RES_PATH, TEST_RES_PATH);
  }
}
