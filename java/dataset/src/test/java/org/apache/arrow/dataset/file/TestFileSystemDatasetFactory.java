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

package org.apache.arrow.dataset.file;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.dataset.jni.BaseMemoryPool;
import org.apache.arrow.dataset.jni.JniWrapper;
import org.apache.arrow.dataset.jni.MemoryPool;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Test;

public class TestFileSystemDatasetFactory {

  @Test
  public void testErrorHandling() {
    RuntimeException e = assertThrows(RuntimeException.class, () -> {
      new FileSystemDatasetFactory(new RootAllocator(Long.MAX_VALUE),
          FileFormat.NONE, "file:///NON_EXIST_FILE");
    });
    assertEquals("illegal file format id: -1", e.getMessage());
  }

  @Test
  public void testCloseAgain() {
    assertDoesNotThrow(() -> {
      FileSystemDatasetFactory factory = new FileSystemDatasetFactory(new RootAllocator(Long.MAX_VALUE),
          FileFormat.PARQUET, "file:///NON_EXIST_FILE");
      factory.close();
      factory.close();
    });
  }

  @Test
  public void benchmarkMemoryPools() {
    int iteration = 1000000;
    BaseMemoryPool pool = new MemoryPool(new RootAllocator(Long.MAX_VALUE));

    long prev;
    prev = System.nanoTime();
    JniWrapper.get().benchSystemMemoryPool(iteration);
    System.out.println("SystemMemoryPool: " + (System.nanoTime() - prev) / 1000000.0D);

    prev = System.nanoTime();
    JniWrapper.get().benchBridgedMemoryPool(iteration, pool);
    System.out.println("BridgedMemoryPool: " + (System.nanoTime() - prev) / 1000000.0D);
  }
}
