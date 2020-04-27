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

import org.junit.Assert;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class OwnershipsTest {

  @Test
  public void testTakeOwnerShip() {
    RootAllocator root = new RootAllocator(Long.MAX_VALUE);

    ChildAllocator allocator1 = (ChildAllocator) root.newChildAllocator("allocator1", 0, Long.MAX_VALUE);
    ChildAllocator allocator2 = (ChildAllocator) root.newChildAllocator("allocator2", 0, Long.MAX_VALUE);
    Assert.assertEquals(0, allocator1.getAllocatedMemory());
    Assert.assertEquals(0, allocator2.getAllocatedMemory());

    int size = 512;

    ArrowBuf buffer = allocator1.buffer(size);
    AllocationManager am = ((BufferLedger) buffer.getReferenceManager()).getAllocationManager();
    BufferLedger owningLedger = am.getOwningLedger();
    Assert.assertEquals(size, owningLedger.getAccountedSize());
    Assert.assertEquals(size, owningLedger.getSize());
    Assert.assertEquals(size, allocator1.getAllocatedMemory());

    BufferLedger transferredLedger = Ownerships.get().takeOwnership(allocator2, am);
    Assert.assertEquals(0, owningLedger.getAccountedSize());
    Assert.assertEquals(size, owningLedger.getSize());
    Assert.assertEquals(size, transferredLedger.getAccountedSize());
    Assert.assertEquals(size, transferredLedger.getSize());
    Assert.assertEquals(0, allocator1.getAllocatedMemory());
    Assert.assertEquals(size, allocator2.getAllocatedMemory());

  }
}
