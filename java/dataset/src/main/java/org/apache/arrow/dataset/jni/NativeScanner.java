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

package org.apache.arrow.dataset.jni;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.SchemaUtility;

/**
 * Native implementation of {@link Scanner}. Note that it currently emits only a single scan task of type
 * {@link NativeScanTask}, which is internally a combination of all scan task instances returned by the
 * native scanner.
 */
public class NativeScanner implements Scanner {

  private final NativeContext context;
  private final long scannerId;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public NativeScanner(NativeContext context, long scannerId) {
    this.context = context;
    this.scannerId = scannerId;
  }

  NativeContext getContext() {
    return context;
  }

  long getId() {
    return scannerId;
  }

  @Override
  public Iterable<? extends ScanTask> scan() {
    return Collections.singletonList(new NativeScanTask(this));
  }

  @Override
  public Schema schema() {
    try {
      return SchemaUtility.deserialize(JniWrapper.get().getSchemaFromScanner(scannerId), context.getAllocator());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    JniWrapper.get().closeScanner(scannerId);
  }
}
