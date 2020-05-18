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

import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferLedger;
import org.apache.arrow.memory.NativeUnderlingMemory;
import org.apache.arrow.memory.Ownerships;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

/**
 * Native implementation of {@link ScanTask}. Currently RecordBatches are iterated directly by the scanner
 * id via {@link JniWrapper}, thus we allow only one-time execution of method {@link #scan()}. If a re-scan
 * operation is expected, call {@link NativeDataset#newScan} to create a new scanner instance.
 */
public class NativeScanTask implements ScanTask, AutoCloseable {
  private final NativeScanner scanner;
  private final AtomicBoolean executed = new AtomicBoolean(false);

  /**
   * Constructor.
   */
  public NativeScanTask(NativeScanner scanner) {
    this.scanner = scanner;
  }

  @Override
  public BatchIterator scan() {
    if (!executed.compareAndSet(false, true)) {
      throw new UnsupportedOperationException("Method scan() cannot be executed more than once if it is a " +
          "native scan task");
    }
    return new BatchIterator() {
      private ArrowRecordBatch peek = null;

      @Override
      public void close() {
        scanner.close();
      }

      @Override
      public boolean hasNext() {
        if (peek != null) {
          return true;
        }
        NativeRecordBatchHandle handle = JniWrapper.get().nextRecordBatch(scanner.getId());
        if (handle == null) {
          return false;
        }
        final ArrayList<ArrowBuf> buffers = new ArrayList<>();
        for (NativeRecordBatchHandle.Buffer buffer : handle.getBuffers()) {
          final BaseAllocator allocator = scanner.getContext().getAllocator();
          final NativeUnderlingMemory am = new NativeUnderlingMemory(allocator,
              (int) buffer.size, buffer.nativeInstanceId, buffer.memoryAddress);
          final BufferLedger ledger = Ownerships.get().takeOwnership(allocator, am);
          ArrowBuf buf = new ArrowBuf(ledger, null, (int) buffer.size, buffer.memoryAddress, false);
          buffers.add(buf);
        }

        try {
          peek = new ArrowRecordBatch((int) handle.getNumRows(), handle.getFields().stream()
              .map(field -> new ArrowFieldNode((int) field.length, (int) field.nullCount))
              .collect(Collectors.toList()), buffers);
          return true;
        } finally {
          buffers.forEach(buffer -> buffer.getReferenceManager().release());
        }
      }

      @Override
      public ArrowRecordBatch next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        try {
          return peek;
        } finally {
          peek = null;
        }
      }
    };
  }

  @Override
  public void close() {
    scanner.close();
  }
}
