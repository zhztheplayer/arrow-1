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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BaseAllocator;

public class MemoryPool implements BaseMemoryPool {
  private final BaseAllocator allocator;
  private final ConcurrentMap<Long, ArrowBuf> allocatedBuffers = new ConcurrentHashMap<>();
  private final ArrowBuf empty;

  public MemoryPool(BaseAllocator allocator) {
    this.allocator = allocator;
    this.empty = allocator.getEmpty();
  }

  @Override
  public long allocate(long size) {
    return allocateNew(size);
  }

  private long allocateNew(long size) {
    if (size == 0) {
      // allocate empty buffer
      return empty.memoryAddress();
    }
    final ArrowBuf buf = allocator.buffer(size);
    final long address = buf.memoryAddress();
    allocatedBuffers.put(address, buf);
    return address;
  }

  @Override
  public long reallocate(long memoryAddress, long oldSize, long newSize) {
    if (memoryAddress == empty.memoryAddress()) {
      if (oldSize != 0) {
        throw new IllegalStateException("oldSize of the default empty buffer should be 0");
      }
      return allocateNew(newSize);
    }
    final ArrowBuf buf = allocatedBuffers.get(memoryAddress);
    if (buf == null) {
      throw new IllegalStateException("Reallocate on unallocated memory address");
    }
    if (oldSize != buf.capacity()) {
      throw new IllegalStateException("Reallocate with mismatched old size");
    }
    if (newSize <= oldSize) {
      reduceSize(buf, newSize);
      return buf.memoryAddress();
    }
    // newSize > oldSize. Create new buffer and free the old one.
    buf.getReferenceManager().release();
    allocatedBuffers.remove(memoryAddress);
    return allocateNew(newSize);
  }

  private void reduceSize(ArrowBuf buf, long newSize) {
    long oldAddress = buf.memoryAddress();
    buf.capacity(newSize);
    long newAddress = buf.memoryAddress();
    if (newAddress != oldAddress) {
      throw new IllegalStateException("Memory address changed after resizing to smaller size");
    }
  }

  @Override
  public void free(long memoryAddress, long freeSize) {
    if (memoryAddress == empty.memoryAddress()) {
      if (freeSize > 0) {
        throw new IllegalStateException("Free non-zero bytes on default empty buffer");
      }
      return;
    }
    final ArrowBuf buf = allocatedBuffers.get(memoryAddress);
    if (buf == null) {
      throw new IllegalStateException("Free on unallocated memory address");
    }
    if (freeSize > buf.capacity()) {
      throw new IllegalStateException("freeSize is larger than current allocated size");
    }
    if (freeSize == buf.capacity()) {
      buf.getReferenceManager().release();
      allocatedBuffers.remove(memoryAddress);
    }
    // freeSize < buf.capacity. Resize to smaller in place.
    long newSize = buf.capacity() - freeSize;
    reduceSize(buf, newSize);
  }

  public BaseAllocator getAllocator() {
    return allocator;
  }

  public ArrowBuf getAllocatedBuf(long memoryAddress) {
    ArrowBuf buf = allocatedBuffers.get(memoryAddress);
    if (buf == null) {
      throw new IllegalArgumentException("No allocated buffer at memory address " + memoryAddress + " in this pool");
    }
    return buf;
  }

  public ArrowBuf getEmptyBuf() {
    return empty;
  }
}
