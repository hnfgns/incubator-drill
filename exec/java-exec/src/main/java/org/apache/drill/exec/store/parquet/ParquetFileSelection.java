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
package org.apache.drill.exec.store.parquet;

import com.google.common.base.Preconditions;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.parquet.Metadata.ParquetTableMetadata_v1;

/**
 * Parquet specific {@link FileSelection selection} that carries out {@link ParquetTableMetadata_v1 metadata} along.
 */
public class ParquetFileSelection extends FileSelection {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetFileSelection.class);

  private final ParquetTableMetadata_v1 metadata;

  protected ParquetFileSelection(final FileSelection delegate, ParquetTableMetadata_v1 metadata) {
    super(delegate.statuses, delegate.files, delegate.selectionRoot);
    this.metadata = Preconditions.checkNotNull(metadata, "Parquet metadata cannot be null");
  }

  /**
   * Return the parquet table metadata that may have been read
   * from a metadata cache file during creation of this file selection.
   * It will always be null for non-parquet files and null for cases
   * where no metadata cache was created.
   */
  public ParquetTableMetadata_v1 getParquetMetadata() {
    return metadata;
  }

  /**
   * Creates a new Parquet specific selection wrapping the given {@link FileSelection selection}.
   *
   * @param selection  inner file selection
   * @param metadata  parquet metadata
   * @return  null if selection is null
   *          otherwise a new selection
   */
  public static ParquetFileSelection create(final FileSelection selection, final ParquetTableMetadata_v1 metadata) {
    if (selection == null) {
      return null;
    }
    return new ParquetFileSelection(selection, metadata);
  }

}
