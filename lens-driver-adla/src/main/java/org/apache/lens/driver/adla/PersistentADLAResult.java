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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.lens.driver.jdbc.JDBCResultSet;
import org.apache.lens.server.api.driver.LensResultSetMetadata;
import org.apache.lens.server.api.driver.PersistentResultSet;
import org.apache.lens.server.api.error.LensException;
import org.codehaus.jackson.annotate.JsonIgnore;


public class PersistentADLAResult extends PersistentResultSet {

  String path;

  public PersistentADLAResult(String path) {
    this.path = path;
  }

  @Override
  public Long getFileSize() throws LensException {
    return new File(path).getTotalSpace();
  }

  @Override
  public Integer size() throws LensException {
    return null;
  }

  @Override
  public LensResultSetMetadata getMetadata() throws LensException {
    return new ADLAResultSetMetadata();
  }

  @Override
  public String getOutputPath() throws LensException {
    return path;
  }


  private static class ADLAResultSetMetadata extends LensResultSetMetadata {
    @JsonIgnore
    @Override
    public List<ColumnDescriptor> getColumns() {
      return new ArrayList<>();
    }
  }
}
