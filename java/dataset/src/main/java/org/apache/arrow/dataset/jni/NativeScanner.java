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
import java.util.ArrayList;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.arrow.dataset.scanner.ScanTask;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferLedger;
import org.apache.arrow.memory.NativeUnderlingMemory;
import org.apache.arrow.memory.Ownerships;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.SchemaUtility;

/**
 * Native implementation of {@link Scanner}. Note that it currently emits only a single scan task of type
 * {@link NativeScanTask}, which is internally a combination of all scan task instances returned by the
 * native scanner.
 */
public class NativeScanner implements Scanner {

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicBoolean executed = new AtomicBoolean(false);
  private final NativeContext context;
  private final long scannerId;

  public NativeScanner(NativeContext context, long scannerId) {
    this.context = context;
    this.scannerId = scannerId;
  }

  ScanTask.BatchIterator execute() {
    if (!executed.compareAndSet(false, true)) {
      throw new UnsupportedOperationException("NativeScanner cannot be executed more than once. Consider creating " +
          "new scanner instead");
    }
    return new ScanTask.BatchIterator() {
      private ArrowRecordBatch peek = null;

      @Override
      public void close() {
        NativeScanner.this.close();
      }

      @Override
      public boolean hasNext() {
        if (peek != null) {
          return true;
        }
        NativeRecordBatchHandle handle = JniWrapper.get().nextRecordBatch(scannerId);
        if (handle == null) {
          return false;
        }
        final ArrayList<ArrowBuf> buffers = new ArrayList<>();
        for (NativeRecordBatchHandle.Buffer buffer : handle.getBuffers()) {
          final BaseAllocator allocator = context.getAllocator();
          final NativeUnderlingMemory am = new NativeUnderlingMemory(allocator,
              (int) buffer.size, buffer.nativeInstanceId, buffer.memoryAddress);
          final BufferLedger ledger = Ownerships.get().takeOwnership(allocator, am);
          ArrowBuf buf = new ArrowBuf(ledger, null, (int) buffer.size, buffer.memoryAddress);
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
  public Iterable<? extends NativeScanTask> scan() {
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
