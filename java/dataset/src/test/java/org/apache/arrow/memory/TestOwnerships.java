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

package org.apache.arrow.memory;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestOwnerships {

  @Test
  public void testTakeOwnerShip() {
    RootAllocator root = new RootAllocator(Long.MAX_VALUE);

    ChildAllocator allocator1 = (ChildAllocator) root.newChildAllocator("allocator1", 0, Long.MAX_VALUE);
    ChildAllocator allocator2 = (ChildAllocator) root.newChildAllocator("allocator2", 0, Long.MAX_VALUE);
    assertEquals(0, allocator1.getAllocatedMemory());
    assertEquals(0, allocator2.getAllocatedMemory());

    int size = 512;

    ArrowBuf buffer = allocator1.buffer(size);
    AllocationManager am = ((BufferLedger) buffer.getReferenceManager()).getAllocationManager();
    BufferLedger owningLedger = am.getOwningLedger();
    assertEquals(size, owningLedger.getAccountedSize());
    assertEquals(size, owningLedger.getSize());
    assertEquals(size, allocator1.getAllocatedMemory());

    BufferLedger transferredLedger = Ownerships.get().takeOwnership(allocator2, am);
    assertEquals(0, owningLedger.getAccountedSize());
    assertEquals(size, owningLedger.getSize());
    assertEquals(size, transferredLedger.getAccountedSize());
    assertEquals(size, transferredLedger.getSize());
    assertEquals(0, allocator1.getAllocatedMemory());
    assertEquals(size, allocator2.getAllocatedMemory());

  }
}
