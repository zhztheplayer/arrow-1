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

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.arrow.dataset.filter.Filter;
import org.apache.arrow.dataset.jni.TestNativeDatasetScan;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestSingleFileDatasetScan extends TestNativeDatasetScan {

  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();

  @Test
  public void testParquetRead() throws Exception {
    ParquetWriteSupport writeSupport = new ParquetWriteSupport("test.avsc", TMP.newFolder());
    final GenericRecord record = new GenericData.Record(writeSupport.getAvroSchema());
    record.put("id", 1);
    record.put("name", "a");
    writeSupport.writeRecord(record);
    writeSupport.close();

    SingleFileDatasetFactory factory = new SingleFileDatasetFactory(rootAllocator(),
        FileFormat.PARQUET, FileSystem.LOCAL, writeSupport.getOutputFile());
    ScanOptions options = new ScanOptions(new String[0], Filter.EMPTY, 100);

    Schema schema = inferResultSchemaFromFactory(factory, options);
    assertEquals(2, schema.getFields().size());
    assertEquals("id", schema.getFields().get(0).getName());
    assertEquals("name", schema.getFields().get(1).getName());
    assertEquals(Types.MinorType.INT.getType(), schema.getFields().get(0).getType());
    assertEquals(Types.MinorType.VARCHAR.getType(), schema.getFields().get(1).getType());

    assertSingleTaskProduced(factory, options);

    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
    assertEquals(1, datum.size());

    AutoCloseables.close(datum);
  }

}
