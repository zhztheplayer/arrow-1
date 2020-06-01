// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/dataset/api.h>
#include <arrow/dataset/file_base.h>
#include <arrow/filesystem/hdfs.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/iterator.h>

#include "org_apache_arrow_dataset_file_JniWrapper.h"
#include "org_apache_arrow_dataset_jni_JniWrapper.h"

static JavaVM* java_vm;

static jclass illegal_access_exception_class;
static jclass illegal_argument_exception_class;
static jclass runtime_exception_class;

static jclass object_class;
static jclass record_batch_handle_class;
static jclass record_batch_handle_field_class;
static jclass record_batch_handle_buffer_class;
static jclass base_memory_pool_class;

static jmethodID object_to_string;
static jmethodID record_batch_handle_constructor;
static jmethodID record_batch_handle_field_constructor;
static jmethodID record_batch_handle_buffer_constructor;
static jmethodID base_memory_pool_allocate;
static jmethodID base_memory_pool_reallocate;
static jmethodID base_memory_pool_free;

static jint JNI_VERSION = JNI_VERSION_1_6;

class JniPendingException : public std::runtime_error {
 public:
  explicit JniPendingException(const std::string& arg) : runtime_error(arg) {}
};

void ThrowPendingException(const std::string& message) {
  throw JniPendingException(message);
}

template <typename T>
T JniGetOrThrow(arrow::Result<T> result) {
  if (!result.status().ok()) {
    ThrowPendingException(result.status().message());
  }
  return std::move(result).ValueOrDie();
}

void JniAssertOkOrThrow(arrow::Status status) {
  if (!status.ok()) {
    ThrowPendingException(status.message());
  }
}

void JniThrow(std::string message) { ThrowPendingException(message); }

#define JNI_METHOD_START try {
// macro ended

#define JNI_METHOD_END(fallback_expr)                 \
  }                                                   \
  catch (JniPendingException & e) {                   \
    env->ThrowNew(runtime_exception_class, e.what()); \
    return fallback_expr;                             \
  }
// macro ended

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name) {
  jclass local_class = env->FindClass(class_name);
  jclass global_class = (jclass)env->NewGlobalRef(local_class);
  env->DeleteLocalRef(local_class);
  return global_class;
}

arrow::Result<jmethodID> GetMethodID(JNIEnv* env, jclass this_class, const char* name,
                                     const char* sig) {
  jmethodID ret = env->GetMethodID(this_class, name, sig);
  if (ret == nullptr) {
    std::string error_message = "Unable to find method " + std::string(name) +
                                " within signature" + std::string(sig);
    return arrow::Status::Invalid(error_message);
  }
  return ret;
}

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }
  JNI_METHOD_START
  java_vm = vm;
  object_class =
      CreateGlobalClassReference(env, "Ljava/lang/Object;");
  illegal_access_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalAccessException;");
  illegal_argument_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/IllegalArgumentException;");
  runtime_exception_class =
      CreateGlobalClassReference(env, "Ljava/lang/RuntimeException;");

  record_batch_handle_class =
      CreateGlobalClassReference(env,
                                 "Lorg/apache/arrow/"
                                 "dataset/jni/NativeRecordBatchHandle;");
  record_batch_handle_field_class =
      CreateGlobalClassReference(env,
                                 "Lorg/apache/arrow/"
                                 "dataset/jni/NativeRecordBatchHandle$Field;");
  record_batch_handle_buffer_class =
      CreateGlobalClassReference(env,
                                 "Lorg/apache/arrow/"
                                 "dataset/jni/NativeRecordBatchHandle$Buffer;");
  base_memory_pool_class =
      CreateGlobalClassReference(env,
                                 "Lorg/apache/arrow/"
                                 "dataset/jni/BaseMemoryPool;");

  object_to_string = JniGetOrThrow(
      GetMethodID(env, object_class, "toString", "()Ljava/lang/String;"));
  record_batch_handle_constructor =
      JniGetOrThrow(GetMethodID(env, record_batch_handle_class, "<init>",
                                "(J[Lorg/apache/arrow/dataset/"
                                "jni/NativeRecordBatchHandle$Field;"
                                "[Lorg/apache/arrow/dataset/"
                                "jni/NativeRecordBatchHandle$Buffer;)V"));
  record_batch_handle_field_constructor =
      JniGetOrThrow(GetMethodID(env, record_batch_handle_field_class, "<init>", "(JJ)V"));
  record_batch_handle_buffer_constructor = JniGetOrThrow(
      GetMethodID(env, record_batch_handle_buffer_class, "<init>", "(JJJJ)V"));
  base_memory_pool_allocate =
      JniGetOrThrow(GetMethodID(env, base_memory_pool_class, "allocate", "(J)J"));
  base_memory_pool_reallocate =
      JniGetOrThrow(GetMethodID(env, base_memory_pool_class, "reallocate", "(JJJ)J"));
  base_memory_pool_free =
      JniGetOrThrow(GetMethodID(env, base_memory_pool_class, "free", "(JJ)V"));

  return JNI_VERSION;
  JNI_METHOD_END(JNI_ERR)
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
  JNI_METHOD_START
  env->DeleteGlobalRef(object_class);
  env->DeleteGlobalRef(illegal_access_exception_class);
  env->DeleteGlobalRef(illegal_argument_exception_class);
  env->DeleteGlobalRef(runtime_exception_class);
  env->DeleteGlobalRef(record_batch_handle_class);
  env->DeleteGlobalRef(record_batch_handle_field_class);
  env->DeleteGlobalRef(record_batch_handle_buffer_class);
  env->DeleteGlobalRef(base_memory_pool_class);
  JNI_METHOD_END()
}

std::shared_ptr<arrow::Schema> SchemaFromColumnNames(
    const std::shared_ptr<arrow::Schema>& input,
    const std::vector<std::string>& column_names) {
  std::vector<std::shared_ptr<arrow::Field>> columns;
  for (const auto& name : column_names) {
    columns.push_back(input->GetFieldByName(name));
  }
  return std::make_shared<arrow::Schema>(columns);
}

arrow::Result<std::shared_ptr<arrow::dataset::FileFormat>> GetFileFormat(jint id) {
  switch (id) {
    case 0:
      return std::make_shared<arrow::dataset::ParquetFileFormat>();
    default:
      std::string error_message = "illegal file format id: " + std::to_string(id);
      return arrow::Status::Invalid(error_message);
  }
}

std::string JStringToCString(JNIEnv* env, jstring string) {
  if (string == nullptr) {
    return std::string();
  }
  jboolean copied;
  const char* chars = env->GetStringUTFChars(string, &copied);
  std::string ret = strdup(chars);
  env->ReleaseStringUTFChars(string, chars);
  return ret;
}

std::vector<std::string> ToStringVector(JNIEnv* env, jobjectArray& str_array) {
  int length = env->GetArrayLength(str_array);
  std::vector<std::string> vector;
  for (int i = 0; i < length; i++) {
    auto string = (jstring)(env->GetObjectArrayElement(str_array, i));
    vector.push_back(JStringToCString(env, string));
  }
  return vector;
}

template <typename T>
jlong CreateNativeRef(std::shared_ptr<T> t) {
  std::shared_ptr<T>* retained_ptr = new std::shared_ptr<T>(t);
  return reinterpret_cast<jlong>(retained_ptr);
}

template <typename T>
std::shared_ptr<T> RetrieveNativeInstance(jlong ref) {
  std::shared_ptr<T>* retrieved_ptr = reinterpret_cast<std::shared_ptr<T>*>(ref);
  return *retrieved_ptr;
}

template <typename T>
void ReleaseNativeRef(jlong ref) {
  std::shared_ptr<T>* retrieved_ptr = reinterpret_cast<std::shared_ptr<T>*>(ref);
  delete retrieved_ptr;
}

arrow::Result<jbyteArray> ToSchemaByteArray(JNIEnv* env,
                                            std::shared_ptr<arrow::Schema> schema) {
  ARROW_ASSIGN_OR_RAISE(
      std::shared_ptr<arrow::Buffer> buffer,
      arrow::ipc::SerializeSchema(*schema, nullptr, arrow::default_memory_pool()))

  jbyteArray out = env->NewByteArray(buffer->size());
  auto src = reinterpret_cast<const jbyte*>(buffer->data());
  env->SetByteArrayRegion(out, 0, buffer->size(), src);
  return out;
}

arrow::Result<std::shared_ptr<arrow::Schema>> FromSchemaByteArray(
    JNIEnv* env, jbyteArray schemaBytes) {
  arrow::ipc::DictionaryMemo in_memo;
  int schemaBytes_len = env->GetArrayLength(schemaBytes);
  jbyte* schemaBytes_data = env->GetByteArrayElements(schemaBytes, nullptr);
  auto serialized_schema = std::make_shared<arrow::Buffer>(
      reinterpret_cast<uint8_t*>(schemaBytes_data), schemaBytes_len);
  arrow::io::BufferReader buf_reader(serialized_schema);
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema,
                        arrow::ipc::ReadSchema(&buf_reader, &in_memo))
  env->ReleaseByteArrayElements(schemaBytes, schemaBytes_data, JNI_ABORT);
  return schema;
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeDatasetFactory
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeDatasetFactory(
    JNIEnv* env, jobject, jlong id) {
  JNI_METHOD_START
  ReleaseNativeRef<arrow::dataset::DatasetFactory>(id);
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    inspectSchema
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_inspectSchema(
    JNIEnv* env, jobject, jlong dataset_factor_id) {
  JNI_METHOD_START
  std::shared_ptr<arrow::dataset::DatasetFactory> d =
      RetrieveNativeInstance<arrow::dataset::DatasetFactory>(dataset_factor_id);
  std::shared_ptr<arrow::Schema> schema = JniGetOrThrow(d->Inspect());
  return JniGetOrThrow(ToSchemaByteArray(env, schema));
  JNI_METHOD_END(nullptr)
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    createDataset
 * Signature: (J[B)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_createDataset(
    JNIEnv* env, jobject, jlong dataset_factory_id, jbyteArray schema_bytes) {
  JNI_METHOD_START
  std::shared_ptr<arrow::dataset::DatasetFactory> d =
      RetrieveNativeInstance<arrow::dataset::DatasetFactory>(dataset_factory_id);
  std::shared_ptr<arrow::Schema> schema;
  schema = JniGetOrThrow(FromSchemaByteArray(env, schema_bytes));
  std::shared_ptr<arrow::dataset::Dataset> dataset = JniGetOrThrow(d->Finish(schema));
  return CreateNativeRef(dataset);
  JNI_METHOD_END(-1L)
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeDataset
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeDataset(
    JNIEnv* env, jobject, jlong id) {
  JNI_METHOD_START
  ReleaseNativeRef<arrow::dataset::Dataset>(id);
  JNI_METHOD_END()
}

class BridgedMemoryPool : public arrow::MemoryPool {
 public:
  BridgedMemoryPool(jobject memory_pool) {
    this->memory_pool_ = JniGetOrThrow(GetJNIEnv())->NewGlobalRef(memory_pool);
  }

  ~BridgedMemoryPool() override {
    JniGetOrThrow(GetJNIEnv())->DeleteGlobalRef(memory_pool_);
  }

  arrow::Status Allocate(int64_t size, uint8_t **out) override {
    ARROW_ASSIGN_OR_RAISE(JNIEnv* env, GetJNIEnv())
    jlong address = env->CallLongMethod(memory_pool_, base_memory_pool_allocate, size);
    if (env->ExceptionCheck()) {
      jthrowable t = env->ExceptionOccurred();
      env->ExceptionClear();
      jstring message = (jstring) env->CallObjectMethod(t, object_to_string);
      return arrow::Status::Invalid("Error occurred invoking Java method: ",
          JStringToCString(env, message));
    }
    *out = reinterpret_cast<uint8_t *>(address);
    stats_.UpdateAllocatedBytes(size);
    return arrow::Status::OK();
  }

  arrow::Status Reallocate(int64_t old_size, int64_t new_size, uint8_t **ptr) override {
    ARROW_ASSIGN_OR_RAISE(JNIEnv* env, GetJNIEnv())
    jlong newAddress =
        env->CallLongMethod(memory_pool_, base_memory_pool_reallocate,
                             reinterpret_cast<jlong>(*ptr), old_size, new_size);
    if (env->ExceptionCheck()) {
      jthrowable t = env->ExceptionOccurred();
      env->ExceptionClear();
      jstring message = (jstring) env->CallObjectMethod(t, object_to_string);
      return arrow::Status::Invalid("Error occurred invoking Java method: ",
                                    JStringToCString(env, message));
    }
    *ptr = reinterpret_cast<uint8_t *>(newAddress);
    stats_.UpdateAllocatedBytes(new_size - old_size);
    return arrow::Status::OK();
  }

  void Free(uint8_t *buffer, int64_t size) override {
    // status return is not supported here. use JniGetOrThrow instead.
    JNIEnv *env = JniGetOrThrow(GetJNIEnv());
    env->CallVoidMethod(memory_pool_, base_memory_pool_free,
                         reinterpret_cast<jlong>(buffer), size);
    if (env->ExceptionCheck()) {
      jthrowable t = env->ExceptionOccurred();
      env->ExceptionClear();
      jstring message = (jstring) env->CallObjectMethod(t, object_to_string);
      JniThrow("Error occurred invoking Java method: " + JStringToCString(env, message));
    }
    stats_.UpdateAllocatedBytes(-size);
  }

  int64_t bytes_allocated() const override {
    return stats_.bytes_allocated();
  }

  int64_t max_memory() const override {
    return stats_.max_memory();
  }

  std::string backend_name() const override {
    return "bridged";
  }

 private:
  jobject memory_pool_;
  arrow::internal::MemoryPoolStats stats_;

  arrow::Result<JNIEnv*> GetJNIEnv() {
    JNIEnv* env;
    jint status = java_vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
    if (status == JNI_EDETACHED) {
      return arrow::Status::Invalid("JNI has detached from current thread");
    }
    return env;
  }
};

/// \class DisposableScannerAdaptor
/// \brief An adaptor that iterates over a Scanner instance then returns RecordBatches
/// directly.
///
/// This lessens the complexity of the JNI bridge to make sure it to be easier to
/// maintain. On Java-side, NativeScanner can only produces a single NativeScanTask
/// instance during its whole lifecycle. Each task stands for a DisposableScannerAdaptor
/// instance through JNI bridge.
///
class DisposableScannerAdaptor {
 public:
  DisposableScannerAdaptor(std::shared_ptr<arrow::dataset::Scanner> scanner,
      arrow::dataset::ScanTaskIterator task_itr,
      std::shared_ptr<BridgedMemoryPool> pool) {
    this->scanner_ = std::move(scanner);
    this->task_itr_ = std::move(task_itr);
    this->pool_ = std::move(pool);
  }

  static arrow::Result<std::shared_ptr<DisposableScannerAdaptor>> Create(
      std::shared_ptr<arrow::dataset::Scanner> scanner,
      std::shared_ptr<BridgedMemoryPool> pool) {
    ARROW_ASSIGN_OR_RAISE(arrow::dataset::ScanTaskIterator task_itr, scanner->Scan())
    return std::make_shared<DisposableScannerAdaptor>(scanner, std::move(task_itr), pool);
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> Next() {
    do {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> batch, NextBatch())
      if (batch != nullptr) {
        return batch;
      }
      // batch is null, current task is fully consumed
      ARROW_ASSIGN_OR_RAISE(bool has_next_task, NextTask())
      if (!has_next_task) {
        // no more tasks
        return nullptr;
      }
      // new task appended, read again
    } while (true);
  }

  const std::shared_ptr<arrow::dataset::Scanner>& GetScanner() const { return scanner_; }

  const std::shared_ptr<BridgedMemoryPool>& GetPool() const {
    return pool_;
  }

 protected:
  std::shared_ptr<BridgedMemoryPool> pool_;
  std::shared_ptr<arrow::dataset::Scanner> scanner_;
  arrow::dataset::ScanTaskIterator task_itr_;
  std::shared_ptr<arrow::dataset::ScanTask> current_task_ = nullptr;
  arrow::RecordBatchIterator current_batch_itr_ =
      arrow::MakeEmptyIterator<std::shared_ptr<arrow::RecordBatch>>();

  arrow::Result<bool> NextTask() {
    ARROW_ASSIGN_OR_RAISE(current_task_, task_itr_.Next())
    if (current_task_ == nullptr) {
      return false;
    }
    ARROW_ASSIGN_OR_RAISE(current_batch_itr_, current_task_->Execute())
    return true;
  }

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> NextBatch() {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> batch,
                          current_batch_itr_.Next())
    return batch;
  }
};

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    createScanner
 * Signature: (J[Ljava/lang/String;JLorg/apache/arrow/dataset/jni/BaseMemoryPool;)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_createScanner(
    JNIEnv* env, jobject, jlong dataset_id, jobjectArray columns, jlong batch_size,
    jobject memory_pool) {
  JNI_METHOD_START
  std::shared_ptr<arrow::dataset::ScanContext> context =
      std::make_shared<arrow::dataset::ScanContext>();
  std::shared_ptr<BridgedMemoryPool> bridged_pool =
      std::make_shared<BridgedMemoryPool>(memory_pool);
  context->pool = bridged_pool.get();
  std::shared_ptr<arrow::dataset::Dataset> dataset =
      RetrieveNativeInstance<arrow::dataset::Dataset>(dataset_id);
  std::shared_ptr<arrow::dataset::ScannerBuilder> scanner_builder =
      JniGetOrThrow(dataset->NewScan(context));

  std::vector<std::string> column_vector = ToStringVector(env, columns);
  JniAssertOkOrThrow(scanner_builder->Project(column_vector));
  JniAssertOkOrThrow(scanner_builder->BatchSize(batch_size));

  auto scanner = JniGetOrThrow(scanner_builder->Finish());
  std::shared_ptr<DisposableScannerAdaptor> scanner_adaptor =
      JniGetOrThrow(DisposableScannerAdaptor::Create(scanner, bridged_pool));
  jlong id = CreateNativeRef(scanner_adaptor);
  return id;
  JNI_METHOD_END(-1L)
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    closeScanner
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_closeScanner(
    JNIEnv* env, jobject, jlong scanner_id) {
  JNI_METHOD_START
  ReleaseNativeRef<DisposableScannerAdaptor>(scanner_id);
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    getSchemaFromScanner
 * Signature: (J)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_org_apache_arrow_dataset_jni_JniWrapper_getSchemaFromScanner(JNIEnv* env, jobject,
                                                                  jlong scanner_id) {
  JNI_METHOD_START
  std::shared_ptr<arrow::Schema> schema =
      RetrieveNativeInstance<DisposableScannerAdaptor>(scanner_id)
          ->GetScanner()
          ->schema();
  return JniGetOrThrow(ToSchemaByteArray(env, schema));
  JNI_METHOD_END(nullptr)
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    nextRecordBatch
 * Signature: (J)Lorg/apache/arrow/dataset/jni/NativeRecordBatchHandle;
 */
JNIEXPORT jobject JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_nextRecordBatch(
    JNIEnv* env, jobject, jlong scanner_id) {
  JNI_METHOD_START
  std::shared_ptr<DisposableScannerAdaptor> scanner_adaptor =
      RetrieveNativeInstance<DisposableScannerAdaptor>(scanner_id);

  std::shared_ptr<arrow::RecordBatch> record_batch =
      JniGetOrThrow(scanner_adaptor->Next());
  if (record_batch == nullptr) {
    return nullptr;  // stream ended
  }
  std::shared_ptr<arrow::Schema> schema = record_batch->schema();
  jobjectArray field_array =
      env->NewObjectArray(schema->num_fields(), record_batch_handle_field_class, nullptr);

  std::vector<std::shared_ptr<arrow::Buffer>> buffers;
  for (int i = 0; i < schema->num_fields(); ++i) {
    auto column = record_batch->column(i);
    auto dataArray = column->data();
    jobject field = env->NewObject(record_batch_handle_field_class,
                                   record_batch_handle_field_constructor,
                                   column->length(), column->null_count());
    env->SetObjectArrayElement(field_array, i, field);

    for (auto& buffer : dataArray->buffers) {
      buffers.push_back(buffer);
    }
  }

  jobjectArray buffer_array =
      env->NewObjectArray(buffers.size(), record_batch_handle_buffer_class, nullptr);

  for (size_t j = 0; j < buffers.size(); ++j) {
    auto buffer = buffers[j];
    uint8_t* data = nullptr;
    int64_t size = 0;
    int64_t capacity = 0;
    if (buffer != nullptr) {
      data = (uint8_t*)buffer->data();
      size = buffer->size();
      capacity = buffer->capacity();
    }
    jobject buffer_handle = env->NewObject(record_batch_handle_buffer_class,
                                           record_batch_handle_buffer_constructor,
                                           CreateNativeRef(buffer), data, size, capacity);
    env->SetObjectArrayElement(buffer_array, j, buffer_handle);
  }

  jobject ret = env->NewObject(record_batch_handle_class, record_batch_handle_constructor,
                               record_batch->num_rows(), field_array, buffer_array);
  return ret;
  JNI_METHOD_END(nullptr)
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    releaseBuffer
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_releaseBuffer(
    JNIEnv* env, jobject, jlong id) {
  JNI_METHOD_START
  ReleaseNativeRef<arrow::Buffer>(id);
  JNI_METHOD_END()
}

/*
 * Class:     org_apache_arrow_dataset_file_JniWrapper
 * Method:    makeFileSystemDatasetFactory
 * Signature: (Ljava/lang/String;II)J
 */
JNIEXPORT jlong JNICALL
Java_org_apache_arrow_dataset_file_JniWrapper_makeFileSystemDatasetFactory(
    JNIEnv* env, jobject, jstring uri, jint file_format_id) {
  JNI_METHOD_START
  std::shared_ptr<arrow::dataset::FileFormat> file_format =
      JniGetOrThrow(GetFileFormat(file_format_id));
  arrow::dataset::FileSystemFactoryOptions options;
  std::shared_ptr<arrow::dataset::DatasetFactory> d =
      JniGetOrThrow(arrow::dataset::FileSystemDatasetFactory::Make(
          JStringToCString(env, uri), file_format, options));
  return CreateNativeRef(d);
  JNI_METHOD_END(-1L)
}

void bench(int iteration, arrow::MemoryPool *pool) {
  for (int i = 0; i < iteration; i++) {
    uint8_t *out;
    pool->Allocate(64, &out);
    pool->Reallocate(64, 128, &out);
    pool->Reallocate(128, 64, &out);
    pool->Reallocate(64, 64 * 1024 * 1024, &out);
    pool->Free(out, 64 * 1024 * 1024);
  }
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    benchSystemMemoryPool
 * Signature: (I)V
 */
void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_benchSystemMemoryPool
    (JNIEnv *, jobject, jint iteration) {
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  bench(iteration, pool);
}

/*
 * Class:     org_apache_arrow_dataset_jni_JniWrapper
 * Method:    benchBridgedMemoryPool
 * Signature: (ILorg/apache/arrow/dataset/jni/BaseMemoryPool;)V
 */
JNIEXPORT void JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_benchBridgedMemoryPool
    (JNIEnv *, jobject, jint iteration, jobject jpool) {
  arrow::MemoryPool *pool = new BridgedMemoryPool(jpool);
  bench(iteration, pool);
}