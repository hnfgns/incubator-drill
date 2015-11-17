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
package org.apache.drill.exec.store.dfs;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Jackson serializable description of a file selection.
 */
public class FileSelection {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSelection.class);

  @JsonIgnore
  public List<FileStatus> statuses;

  public List<String> files;
  public String selectionRoot;

  protected FileSelection(List<FileStatus> statuses, List<String> files, String selectionRoot) {
    final boolean bothNullOrEmpty = (statuses == null || statuses.size() == 0) && (files == null || files.size() == 0);
    Preconditions.checkArgument(bothNullOrEmpty, "Either statuses or files must be supplied not both/node of them");
    this.statuses = statuses;
    this.files = files;
    this.selectionRoot = Preconditions.checkNotNull(selectionRoot);
  }

  public List<FileStatus> getStatuses(final DrillFileSystem fs) throws IOException {
    if (statuses == null) {
      final List<FileStatus> newStatuses = Lists.newArrayList(new FileStatus[files.size()]);
      for (final String pathStr:files) {
        newStatuses.add(fs.getFileStatus(new Path(pathStr)));
      }
      statuses = newStatuses;
    }
    return statuses;
  }

  public List<String> getFiles() {
    if (files == null) {
      final List<String> newFiles = Lists.newArrayList(new String[statuses.size()]);
      for (final FileStatus status:statuses) {
        newFiles.add(status.getPath().toString());
      }
      files = newFiles;
    }
    return files;
  }

  public boolean containsDirectories(DrillFileSystem fs) throws IOException {
    for (final FileStatus status : getStatuses(fs)) {
      if (status.isDirectory()) {
        return true;
      }
    }
    return false;
  }

  public FileSelection minusDirectories(DrillFileSystem fs) throws IOException {
    final List<FileStatus> statuses = getStatuses(fs);
    final int total = statuses.size();
    final Path[] paths = new Path[total];
    for (int i=0; i<total; i++) {
      paths[i] = statuses.get(i).getPath();
    }
    final List<FileStatus> allStats = fs.list(true, paths);
    final List<FileStatus> nonDirectories = Lists.newArrayList(Iterables.filter(allStats, new Predicate<FileStatus>() {
      @Override
      public boolean apply(@Nullable FileStatus status) {
        return !status.isDirectory();
      }
    }));

    return create(nonDirectories, null, selectionRoot);
  }

  public FileStatus getFirstPath(DrillFileSystem fs) throws IOException {
    return getStatuses(fs).get(0);
  }

  private static String commonPath(List<FileStatus> statuses) {
    String commonPath = "";
    final int total = statuses.size();
    String[][] folders = new String[total][];
    for (int i = 0; i < total; i++) {
      folders[i] = Path.getPathWithoutSchemeAndAuthority(statuses.get(i).getPath()).toString().split("/");
    }
    for (int j = 0; j < folders[0].length; j++) {
      String thisFolder = folders[0][j];
      boolean allMatched = true;
      for (int i = 1; i < folders.length && allMatched; i++) {
        if (folders[i].length < j) {
          allMatched = false;
          break;
        }
        allMatched &= folders[i][j].equals(thisFolder);
      }
      if (allMatched) {
        commonPath += thisFolder + "/";
      } else {
        break;
      }
    }
    URI oneURI = statuses.get(0).getPath().toUri();
    return new Path(oneURI.getScheme(), oneURI.getAuthority(), commonPath).toString();
  }

  public static FileSelection create(final DrillFileSystem fs, final String parent, final String path) throws IOException {
    final Path combined = new Path(parent, removeLeadingSlash(path));
    final FileStatus[] statuses = fs.globStatus(combined);
    return create(Lists.newArrayList(statuses), null, combined.toUri().toString());
  }

  /**
   * Creates a {@link FileSelection selection} out of given file statuses/files and selection root.
   *
   * @param statuses  list of file statuses
   * @param files  list of files
   * @param root  root path for selections
   *
   * @return  null if the list of statuses and files are null or empty
   *          otherwise a new selection.
   */
  public static FileSelection create(final List<FileStatus> statuses, final List<String> files, final String root) {
    if ((statuses == null || statuses.size() == 0) && (files == null || files.size() == 0)) {
      return null;
    }
    final String selectionRoot = Strings.isNullOrEmpty(root) ? commonPath(statuses) : root;
    return new FileSelection(statuses, files, selectionRoot);
  }

  private static String removeLeadingSlash(String path) {
    if (path.charAt(0) == '/') {
      String newPath = path.substring(1);
      return removeLeadingSlash(newPath);
    } else {
      return path;
    }
  }

}
