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

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Context for relevant classes of NativeDataSource.
 */
public class NativeContext {
  private final Schema schema;
  private final BufferAllocator allocator;

  /**
   * Constructor.
   *
   * @param schema Expected schema of the returned {@link org.apache.arrow.vector.VectorSchemaRoot}s.
   * @param allocator The allocator in use.
   */
  public NativeContext(Schema schema, BufferAllocator allocator) {
    Preconditions.checkArgument(allocator instanceof BaseAllocator,
        "currently only instance of BaseAllocator supported");
    this.schema = schema;
    this.allocator = allocator;
  }

  /**
   * Returns the expected schema of the returned {@link org.apache.arrow.vector.VectorSchemaRoot}s.
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * Returns the allocator which is in use.
   */
  public BaseAllocator getAllocator() {
    return (BaseAllocator) allocator;
  }
}
