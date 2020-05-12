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
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.arrow.dataset.DatasetTypes;
import org.apache.arrow.dataset.filter.Filter;
import org.apache.arrow.dataset.jni.TestNativeDatasetScan;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableList;

public class TestSingleFileDatasetScan extends TestNativeDatasetScan {

  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();

  public static final String AVRO_SCHEMA_USER = "user.avsc";

  @Test
  public void testParquetRead() throws Exception {
    ParquetWriteSupport writeSupport = new ParquetWriteSupport(AVRO_SCHEMA_USER, TMP.newFolder());
    GenericRecord record = new GenericData.Record(writeSupport.getAvroSchema());
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
    checkParquetReadResult(schema, Collections.singletonList(record), datum);
    AutoCloseables.close(datum);
  }

  @Test
  public void testParquetProjector() throws Exception {
    ParquetWriteSupport writeSupport = new ParquetWriteSupport(AVRO_SCHEMA_USER, TMP.newFolder());
    GenericRecord record = new GenericData.Record(writeSupport.getAvroSchema());
    record.put("id", 1);
    record.put("name", "a");
    writeSupport.writeRecord(record);
    writeSupport.close();

    SingleFileDatasetFactory factory = new SingleFileDatasetFactory(rootAllocator(),
        FileFormat.PARQUET, FileSystem.LOCAL, writeSupport.getOutputFile());
    ScanOptions options = new ScanOptions(new String[]{"id"}, Filter.EMPTY, 100);

    Schema schema = inferResultSchemaFromFactory(factory, options);
    assertEquals(1, schema.getFields().size());
    assertEquals("id", schema.getFields().get(0).getName());
    assertEquals(Types.MinorType.INT.getType(), schema.getFields().get(0).getType());

    assertSingleTaskProduced(factory, options);

    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
    assertEquals(1, datum.size());
    org.apache.avro.Schema expectedSchema = truncateAvroSchema(writeSupport.getAvroSchema(), 0, 1);
    checkParquetReadResult(schema,
        Collections.singletonList(
            new GenericRecordBuilder(
                expectedSchema)
                .set("id", 1)
                .build()), datum);
    AutoCloseables.close(datum);
  }

  @Test
  public void testParquetFilter() throws Exception {
    ParquetWriteSupport writeSupport = new ParquetWriteSupport(AVRO_SCHEMA_USER, TMP.newFolder());
    List<GenericRecord> records = newGenericRecordListBuilder()
        .add(new GenericRecordBuilder(writeSupport.getAvroSchema())
            .set("id", 1)
            .set("name", "a")
            .build())
        .add(new GenericRecordBuilder(writeSupport.getAvroSchema())
            .set("id", 2)
            .set("name", "b")
            .build())
        .add(new GenericRecordBuilder(writeSupport.getAvroSchema())
            .set("id", 3)
            .set("name", "c")
            .build())
        .build();
    writeSupport.writeRecords(records);
    writeSupport.close();
    SingleFileDatasetFactory factory = new SingleFileDatasetFactory(rootAllocator(),
        FileFormat.PARQUET, FileSystem.LOCAL, writeSupport.getOutputFile());
    Filter filter = new Filter(DatasetTypes.Condition.newBuilder()
        .setRoot(DatasetTypes.TreeNode.newBuilder()
            .setCpNode(DatasetTypes.ComparisonNode.newBuilder()
                .setOpName("equal")
                .setLeftArg(
                    DatasetTypes.TreeNode.newBuilder().setFieldNode(
                        DatasetTypes.FieldNode.newBuilder().setName("id").build()).build())
                .setRightArg(
                    DatasetTypes.TreeNode.newBuilder().setIntNode(
                        DatasetTypes.IntNode.newBuilder().setValue(2).build()).build())
                .build())
            .build())
        .build());
    ScanOptions options = new ScanOptions(new String[0], filter, 100);
    Schema schema = inferResultSchemaFromFactory(factory, options);
    assertSingleTaskProduced(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
    assertEquals(1, datum.size());
    GenericRecord record = new GenericData.Record(writeSupport.getAvroSchema());
    record.put("id", 2);
    record.put("name", "b");
    checkParquetReadResult(schema, Collections.singletonList(record), datum);
    AutoCloseables.close(datum);
  }

  @Test
  public void testParquetBatchSize() throws Exception {
    ParquetWriteSupport writeSupport = new ParquetWriteSupport(AVRO_SCHEMA_USER, TMP.newFolder());
    List<GenericRecord> records = newGenericRecordListBuilder()
        .add(new GenericRecordBuilder(writeSupport.getAvroSchema())
            .set("id", 1)
            .set("name", "a")
            .build())
        .add(new GenericRecordBuilder(writeSupport.getAvroSchema())
            .set("id", 2)
            .set("name", "b")
            .build())
        .add(new GenericRecordBuilder(writeSupport.getAvroSchema())
            .set("id", 3)
            .set("name", "c")
            .build())
        .build();
    writeSupport.writeRecords(records);
    writeSupport.close();
    ScanOptions options = new ScanOptions(new String[0], Filter.EMPTY, 1);
    SingleFileDatasetFactory factory = new SingleFileDatasetFactory(rootAllocator(),
        FileFormat.PARQUET, FileSystem.LOCAL, writeSupport.getOutputFile());
    Schema schema = inferResultSchemaFromFactory(factory, options);
    assertSingleTaskProduced(factory, options);
    List<ArrowRecordBatch> datum = collectResultFromFactory(factory, options);
    assertEquals(3, datum.size());
    datum.forEach(batch -> assertEquals(1, batch.getLength()));
    checkParquetReadResult(schema, records, datum);
    AutoCloseables.close(datum);
  }

  private void checkParquetReadResult(Schema schema, List<GenericRecord> expected, List<ArrowRecordBatch> actual) {
    assertEquals(expected.size(), actual.stream()
        .mapToInt(ArrowRecordBatch::getLength)
        .sum());
    final int fieldCount = schema.getFields().size();
    LinkedList<GenericRecord> expectedRemovable = new LinkedList<>(expected);
    try (VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, rootAllocator())) {
      VectorLoader loader = new VectorLoader(vsr);
      for (ArrowRecordBatch batch : actual) {
        try {
          assertEquals(fieldCount, batch.getNodes().size());
          loader.load(batch);
          int batchRowCount = vsr.getRowCount();
          for (int i = 0; i < fieldCount; i++) {
            FieldVector vector = vsr.getVector(i);
            for (int j = 0; j < batchRowCount; j++) {
              Object object = vector.getObject(j);
              Object expectedObject = expectedRemovable.get(j).get(i);
              assertEquals(Objects.toString(expectedObject),
                  Objects.toString(object));
            }
          }
          for (int i = 0; i < batchRowCount; i++) {
            expectedRemovable.poll();
          }
        } finally {
          batch.close();
        }
      }
      assertTrue(expectedRemovable.isEmpty());
    }
  }

  private ImmutableList.Builder<GenericRecord> newGenericRecordListBuilder() {
    return new ImmutableList.Builder<>();
  }

  private org.apache.avro.Schema truncateAvroSchema(org.apache.avro.Schema schema, int from, int to) {
    List<org.apache.avro.Schema.Field> fields = schema.getFields().subList(from, to);
    return org.apache.avro.Schema.createRecord(
        fields.stream()
            .map(f -> new org.apache.avro.Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order()))
            .collect(Collectors.toList()));
  }
}
