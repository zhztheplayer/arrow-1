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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

/**
 * Utility class for writing Parquet files using Avro based tools.
 */
class ParquetWriteSupport implements AutoCloseable {

  private final String path;
  private final ParquetWriter<GenericRecord> writer;
  private final Schema avroSchema;

  public ParquetWriteSupport(String schemaName, File outputFolder) throws Exception {
    avroSchema = getAvroSchema(schemaName);
    path = outputFolder.getPath() + File.separator + "generated.parquet";
    writer = AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(path))
        .withSchema(avroSchema)
        .build();
  }

  public void writeRecord(GenericRecord record) throws Exception {
    writer.write(record);
  }

  public String getOutputFile() {
    return path;
  }

  public Schema getAvroSchema() {
    return avroSchema;
  }

  private org.apache.avro.Schema getAvroSchema(String schemaName) throws Exception {
    Path schemaPath = Paths.get(TestSingleFileDatasetScan.class.getResource("/").getPath(),
        "avroschema", schemaName);
    return new org.apache.avro.Schema.Parser().parse(schemaPath.toFile());
  }

  @Override
  public void close() throws Exception {
    writer.close();
  }
}
