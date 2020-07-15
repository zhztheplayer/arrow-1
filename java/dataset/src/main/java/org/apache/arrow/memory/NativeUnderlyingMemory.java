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

import org.apache.arrow.dataset.jni.JniWrapper;
import org.apache.arrow.util.Preconditions;

/**
 * AllocationManager implementation for native allocated memory.
 */
public class NativeUnderlyingMemory extends AllocationManager {

  private final int size;
  private final long nativeInstanceId;
  private final long address;

  /**
   * Constructor.
   *
   * @param accountingAllocator The accounting allocator instance
   * @param size Size of underlying memory (in bytes)
   * @param nativeInstanceId ID of the native instance
   */
  NativeUnderlyingMemory(BaseAllocator accountingAllocator, int size, long nativeInstanceId, long address) {
    super(accountingAllocator);
    this.size = size;
    this.nativeInstanceId = nativeInstanceId;
    this.address = address;
    // pre-allocate bytes on accounting allocator
    try (final AllocationReservation reservation = accountingAllocator.newReservation()) {
      reservation.reserve(size);
    } catch (Exception e) {
      release0();
      throw e;
    }
  }

  /**
   * Alias to constructor that assumes specified allocator is an instance of {@link BaseAllocator}.
   */
  public static NativeUnderlyingMemory create(BufferAllocator bufferAllocator, int size, long nativeInstanceId,
      long address) {
    return new NativeUnderlyingMemory(asBaseAllocator(bufferAllocator), size, nativeInstanceId, address);
  }

  public BufferLedger associate(BufferAllocator allocator) {
    return super.associate(asBaseAllocator(allocator));
  }

  @Override
  protected void release0() {
    JniWrapper.get().releaseBuffer(nativeInstanceId);
  }

  @Override
  public long getSize() {
    return size;
  }

  @Override
  protected long memoryAddress() {
    return address;
  }

  private static BaseAllocator asBaseAllocator(BufferAllocator bufferAllocator) {
    Preconditions.checkArgument(bufferAllocator instanceof BaseAllocator,
        "currently only instance of BaseAllocator supported");
    return ((BaseAllocator) bufferAllocator);
  }
}
