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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.memory.RootAllocator;
import org.junit.Test;

public class TestSingleFileDatasetFactory {

  @Test
  public void testErrorHandling1() {
    RuntimeException e = assertThrows(RuntimeException.class, () -> {
      new SingleFileDatasetFactory(new RootAllocator(Long.MAX_VALUE),
          FileFormat.NONE, FileSystem.LOCAL, "NON_EXIST_FILE");
    });
    assertEquals("illegal file format id: -1", e.getMessage());
  }

  @Test
  public void testErrorHandling2() {
    RuntimeException e = assertThrows(RuntimeException.class, () -> {
      new SingleFileDatasetFactory(new RootAllocator(Long.MAX_VALUE),
          FileFormat.PARQUET, FileSystem.NONE, "NON_EXIST_FILE");
    });
    assertEquals("illegal file system id: -1", e.getMessage());
  }
}
