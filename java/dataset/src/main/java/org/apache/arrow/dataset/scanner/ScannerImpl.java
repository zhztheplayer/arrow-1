/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.dataset.scanner;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.arrow.dataset.source.Dataset;

/**
 * A base implementation of Scanner.
 */
public class ScannerImpl implements Scanner {
  private final List<Dataset> sources;
  private final ScanOptions options;

  public ScannerImpl(List<Dataset> sources, ScanOptions options) {
    this.sources = sources;
    this.options = options;
  }

  @Override
  public Iterable<? extends ScanTask> scan() {
    // flat all scan tasks
    // todo low priority: to support filter/projector in Java ScannerImpl
    return sources.stream()
      .flatMap(s -> StreamSupport.stream(s.getFragments(options).spliterator(), false))
      .flatMap(f -> StreamSupport.stream(f.scan().spliterator(), false))
      .collect(Collectors.toList());
  }

  @Override
  public void close() throws Exception {
    sources.forEach(s -> {
      if (s instanceof AutoCloseable) {
        try {
          ((AutoCloseable) s).close();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });
  }
}
