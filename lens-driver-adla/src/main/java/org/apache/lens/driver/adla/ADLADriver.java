/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.driver.adla;

import java.io.*;

import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.driver.adla.translator.SummaryAzureQueryRewriter;
import org.apache.lens.driver.job.states.JobState;
import org.apache.lens.driver.job.utils.JobUtils;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.driver.*;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.events.LensEventListener;
import org.apache.lens.server.api.query.AbstractQueryContext;
import org.apache.lens.server.api.query.PreparedQueryContext;
import org.apache.lens.server.api.query.QueryContext;
import org.apache.lens.server.api.query.cost.QueryCost;
import org.apache.lens.server.api.query.cost.StaticQueryCost;

import org.apache.commons.io.FileUtils;

import org.apache.hadoop.conf.Configuration;

import lombok.extern.slf4j.Slf4j;

@Slf4j

public class ADLADriver extends AbstractLensDriver {

  String localOutputPath;

  SummaryAzureQueryRewriter queryRewriter = new SummaryAzureQueryRewriter();

  @Override
  public void configure(Configuration conf, String driverType, String driverName) throws LensException {
    super.configure(conf, driverType, driverName);
    localOutputPath = getConf().get(LensConfConstants.DRIVER_OUTPUT_LOCAL_PATH);
    log.info("ADLA driver {} configured successfully", getFullyQualifiedName());
  }

  @Override
  public QueryCost estimate(AbstractQueryContext qctx) throws LensException {
    return new StaticQueryCost(1.0d);
  }

  @Override
  public DriverQueryPlan explain(AbstractQueryContext explainCtx) throws LensException {
    throw new LensException("UnSupported operation explain");
  }

  @Override
  public void prepare(PreparedQueryContext pContext) throws LensException {
    throw new LensException("UnSupported operation prepare");
  }

  @Override
  public DriverQueryPlan explainAndPrepare(PreparedQueryContext pContext) throws LensException {
    throw new LensException("UnSupported operation explainAndPrepare");
  }

  @Override
  public void closePreparedQuery(QueryPrepareHandle handle) throws LensException {
    throw new LensException("UnSupported operation closePreparedQuery");
  }

  @Override
  public LensResultSet execute(QueryContext context) throws LensException {
    throw new LensException("UnSupported operation execute");
  }

  @Override
  public void executeAsync(QueryContext context) throws LensException {
    //Submit ADLA JOB
    log.info("Submitting query {} ", context.getQueryHandleString());
    try {
      String usqlQuery = queryRewriter.rewrite(context, this);
      JobUtils.submitJob(context.getQueryHandleString(), usqlQuery, getBearerToken(context));
      log.info("Submitted query {} successfully", context.getQueryHandleString());
    } catch (Exception e) {
      log.error("Filed to submit query {}", context.getQueryHandleString(), e);
    }
  }


  private String getBearerToken(QueryContext context) {
    log.info("bearer token {} ", context.getConf().get("lens.query.bearertoken"));
    return context.getConf().get("lens.query.bearertoken");
  }

/*  private String getUsql(QueryContext context) {

    String dummy = "CREATE EXTERNAL TABLE IF NOT EXISTS wines( id INT,  country STRING,  description STRING,  "
      + "designation STRING,  points INT,  price DECIMAL,  province STRING,  region_1 STRING,  region_2 STRING,"
      + "  variety STRING,  winery STRING) COMMENT ‘Data about wines from a public database’  ROW FORMAT DELIMITED"
      + "    FIELDS TERMINATED BY ‘,’ STORED AS TEXTFILE  "
      + "location ‘adl://puneet879.azuredatalakestore.net/clusters/output/’";
    return dummy + context.getQueryHandleString() + "/result.csv";
  }*/

  @Override
  public void updateStatus(QueryContext context) throws LensException {
    //Update status of ADLA JOB
    log.info("Updating status for query {} ", context.getQueryHandleString());
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      log.error("Error {} stacktrace {}", e.getMessage(),  e.getStackTrace());
    }

    JobState state = null;
    try {
      state = JobUtils.getStatus(context.getQueryHandleString(), getBearerToken(context));
    } catch (Exception e) {
      log.error("Error while getting status for job {}", context.getQueryHandleString(), e);
      context.getDriverStatus().setState(DriverQueryStatus.DriverQueryState.FAILED);
      return;
    }

    if (state.equals(JobState.COMPLETED)){
      log.info("State for job {} is COMPLETED", context.getQueryHandleString());
      context.getDriverStatus().setState(DriverQueryStatus.DriverQueryState.SUCCESSFUL);
    } else if (state.equals(JobState.FAILED)){
      log.error("State for job {} is FAILED", context.getQueryHandleString());
      context.getDriverStatus().setState(DriverQueryStatus.DriverQueryState.FAILED);
    } else if (state.equals(JobState.RUNNING)){
      log.info("State for job {} is RUNNING", context.getQueryHandleString());
      context.getDriverStatus().setState(DriverQueryStatus.DriverQueryState.RUNNING);
    } else if (state.equals(JobState.DOES_NOT_EXIST)){
      log.error("Could not find state for job {}", context.getQueryHandleString());
      context.getDriverStatus().setState(DriverQueryStatus.DriverQueryState.FAILED);
    }
  }

  @Override
  public void closeResultSet(QueryHandle handle) throws LensException {
    //NO OP
  }

  @Override
  public boolean cancelQuery(QueryHandle handle) throws LensException {
    return false;
  }

  @Override
  public void closeQuery(QueryHandle handle) throws LensException {
    //NO OP
  }

  @Override
  public void close() throws LensException {
    //NO OP
  }

  @Override
  public void registerDriverEventListener(LensEventListener<DriverEvent> driverEventListener) {
    // NO OP
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    // NO OP
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // NO OP
  }

  @Override
  protected LensResultSet createResultSet(QueryContext ctx) throws LensException {
    //Get JOB result
    log.info("Creating resultset for query {}", ctx.getQueryHandleString());

    String bearerToken = getBearerToken(ctx);
    InputStream inputStream = JobUtils.getResult(ctx.getQueryHandle().getHandleIdString(), bearerToken);
    File file = new File(localOutputPath + ctx.getQueryHandle().getHandleIdString()
      + LensConfConstants.DRIVER_OUTPUT_FILE_NAME);
    try {
      FileUtils.copyInputStreamToFile(inputStream, file);
    } catch (IOException e) {
      log.error("Error copying stream to file {}", e.getStackTrace());
    }

    //Get JOB result
    return new PersistentADLAResult(file.getPath());
  }
}
