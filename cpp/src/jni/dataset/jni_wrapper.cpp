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
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/message.h>

#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/cast.h"
#include "arrow/compute/kernels/compare.h"
#include "jni/dataset/Types.pb.h"

#include "org_apache_arrow_dataset_file_JniWrapper.h"
#include "org_apache_arrow_dataset_jni_JniWrapper.h"

static jclass illegal_access_exception_class;
static jclass illegal_argument_exception_class;
static jclass runtime_exception_class;

static jclass record_batch_handle_class;
static jclass record_batch_handle_field_class;
static jclass record_batch_handle_buffer_class;

static jmethodID record_batch_handle_constructor;
static jmethodID record_batch_handle_field_constructor;
static jmethodID record_batch_handle_buffer_constructor;

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

  return JNI_VERSION;
  JNI_METHOD_END(JNI_ERR)
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);
  env->DeleteGlobalRef(illegal_access_exception_class);
  env->DeleteGlobalRef(illegal_argument_exception_class);
  env->DeleteGlobalRef(runtime_exception_class);
  env->DeleteGlobalRef(record_batch_handle_class);
  env->DeleteGlobalRef(record_batch_handle_field_class);
  env->DeleteGlobalRef(record_batch_handle_buffer_class);
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

arrow::Result<std::shared_ptr<arrow::fs::FileSystem>> GetFileSystem(
    jint id, std::string path, std::string* out_path) {
  switch (id) {
    case 0:
      *out_path = path;
      return std::make_shared<arrow::fs::LocalFileSystem>();
    case 1: {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::fs::FileSystem> ret,
                            arrow::fs::FileSystemFromUri(path, out_path))
      return ret;
    }
    default:
      std::string error_message = "illegal filesystem id: " + std::to_string(id);
      return arrow::Status::Invalid(error_message);
  }
}

std::string JStringToCString(JNIEnv* env, jstring string) {
  if (string == nullptr) {
    return std::string();
  }
  jboolean copied;
  int32_t length = env->GetStringUTFLength(string);
  const char* chars = env->GetStringUTFChars(string, &copied);
  std::string str = std::string(chars, length);
  // fixme calling ReleaseStringUTFChars if memory leak faced
  return str;
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

bool ParseProtobuf(uint8_t* buf, int bufLen, google::protobuf::Message* msg) {
  google::protobuf::io::CodedInputStream cis(buf, bufLen);
  cis.SetRecursionLimit(1000);
  return msg->ParseFromCodedStream(&cis);
}

void releaseFilterInput(jbyteArray condition_arr, jbyte* condition_bytes, JNIEnv* env) {
  env->ReleaseByteArrayElements(condition_arr, condition_bytes, JNI_ABORT);
}

// fixme in development. Not all node types considered.
arrow::Result<std::shared_ptr<arrow::dataset::Expression>> TranslateNode(
    arrow::dataset::types::TreeNode node) {
  if (node.has_fieldnode()) {
    const arrow::dataset::types::FieldNode& f_node = node.fieldnode();
    const std::string& name = f_node.name();
    return std::make_shared<arrow::dataset::FieldExpression>(name);
  }
  if (node.has_intnode()) {
    const arrow::dataset::types::IntNode& int_node = node.intnode();
    int32_t val = int_node.value();
    return std::make_shared<arrow::dataset::ScalarExpression>(
        std::make_shared<arrow::Int32Scalar>(val));
  }
  if (node.has_longnode()) {
    const arrow::dataset::types::LongNode& long_node = node.longnode();
    int64_t val = long_node.value();
    return std::make_shared<arrow::dataset::ScalarExpression>(
        std::make_shared<arrow::Int64Scalar>(val));
  }
  if (node.has_floatnode()) {
    const arrow::dataset::types::FloatNode& float_node = node.floatnode();
    float_t val = float_node.value();
    return std::make_shared<arrow::dataset::ScalarExpression>(
        std::make_shared<arrow::FloatScalar>(val));
  }
  if (node.has_doublenode()) {
    const arrow::dataset::types::DoubleNode& double_node = node.doublenode();
    double_t val = double_node.value();
    return std::make_shared<arrow::dataset::ScalarExpression>(
        std::make_shared<arrow::DoubleScalar>(val));
  }
  if (node.has_booleannode()) {
    const arrow::dataset::types::BooleanNode& boolean_node = node.booleannode();
    bool val = boolean_node.value();
    return std::make_shared<arrow::dataset::ScalarExpression>(
        std::make_shared<arrow::BooleanScalar>(val));
  }
  if (node.has_andnode()) {
    const arrow::dataset::types::AndNode& and_node = node.andnode();
    const arrow::dataset::types::TreeNode& left_arg = and_node.leftarg();
    const arrow::dataset::types::TreeNode& right_arg = and_node.rightarg();
    ARROW_ASSIGN_OR_RAISE(const std::shared_ptr<arrow::dataset::Expression>& left_expr,
                          TranslateNode(left_arg))
    ARROW_ASSIGN_OR_RAISE(const std::shared_ptr<arrow::dataset::Expression>& right_expr,
                          TranslateNode(right_arg))
    return std::make_shared<arrow::dataset::AndExpression>(left_expr, right_expr);
  }
  if (node.has_ornode()) {
    const arrow::dataset::types::OrNode& or_node = node.ornode();
    const arrow::dataset::types::TreeNode& left_arg = or_node.leftarg();
    const arrow::dataset::types::TreeNode& right_arg = or_node.rightarg();
    ARROW_ASSIGN_OR_RAISE(const std::shared_ptr<arrow::dataset::Expression>& left_expr,
                          TranslateNode(left_arg))
    ARROW_ASSIGN_OR_RAISE(const std::shared_ptr<arrow::dataset::Expression>& right_expr,
                          TranslateNode(right_arg))
    return std::make_shared<arrow::dataset::OrExpression>(left_expr, right_expr);
  }
  if (node.has_cpnode()) {
    const arrow::dataset::types::ComparisonNode& cp_node = node.cpnode();
    const std::string& op_name = cp_node.opname();
    arrow::compute::CompareOperator op;
    if (op_name == "equal") {
      op = arrow::compute::CompareOperator::EQUAL;
    } else if (op_name == "greaterThan") {
      op = arrow::compute::CompareOperator::GREATER;
    } else if (op_name == "greaterThanOrEqual") {
      op = arrow::compute::CompareOperator::GREATER_EQUAL;
    } else if (op_name == "lessThan") {
      op = arrow::compute::CompareOperator::LESS;
    } else if (op_name == "lessThanOrEqual") {
      op = arrow::compute::CompareOperator::LESS_EQUAL;
    } else {
      std::string error_message = "Unknown operation name in comparison node: " + op_name;
      return arrow::Status::Invalid(error_message);
    }
    const arrow::dataset::types::TreeNode& left_arg = cp_node.leftarg();
    const arrow::dataset::types::TreeNode& right_arg = cp_node.rightarg();
    ARROW_ASSIGN_OR_RAISE(const std::shared_ptr<arrow::dataset::Expression>& left_expr,
                          TranslateNode(left_arg))
    ARROW_ASSIGN_OR_RAISE(const std::shared_ptr<arrow::dataset::Expression>& right_expr,
                          TranslateNode(right_arg))
    return std::make_shared<arrow::dataset::ComparisonExpression>(op, left_expr,
                                                                  right_expr);
  }
  if (node.has_notnode()) {
    const arrow::dataset::types::NotNode& not_node = node.notnode();
    const ::arrow::dataset::types::TreeNode& child = not_node.args();
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Expression> translatedChild,
                          TranslateNode(child))
    return std::make_shared<arrow::dataset::NotExpression>(translatedChild);
  }
  if (node.has_isvalidnode()) {
    const arrow::dataset::types::IsValidNode& is_valid_node = node.isvalidnode();
    const ::arrow::dataset::types::TreeNode& child = is_valid_node.args();
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Expression> translatedChild,
                          TranslateNode(child))
    return std::make_shared<arrow::dataset::IsValidExpression>(translatedChild);
  }
  std::string error_message = "Unknown node type";
  return arrow::Status::Invalid(error_message);
}

arrow::Result<std::shared_ptr<arrow::dataset::Expression>> TranslateFilter(
    arrow::dataset::types::Condition condition) {
  const arrow::dataset::types::TreeNode& tree_node = condition.root();
  return TranslateNode(tree_node);
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
                           arrow::dataset::ScanTaskIterator task_itr) {
    this->scanner_ = std::move(scanner);
    this->task_itr_ = std::move(task_itr);
  }

  static arrow::Result<std::shared_ptr<DisposableScannerAdaptor>> Create(
      std::shared_ptr<arrow::dataset::Scanner> scanner) {
    ARROW_ASSIGN_OR_RAISE(arrow::dataset::ScanTaskIterator task_itr, scanner->Scan())
    return std::make_shared<DisposableScannerAdaptor>(scanner, std::move(task_itr));
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

 protected:
  arrow::dataset::ScanTaskIterator task_itr_;
  std::shared_ptr<arrow::dataset::Scanner> scanner_;
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
 * Signature: (J[Ljava/lang/String;[BJ)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_arrow_dataset_jni_JniWrapper_createScanner(
    JNIEnv* env, jobject, jlong dataset_id, jobjectArray columns, jbyteArray filter,
    jlong batch_size) {
  JNI_METHOD_START
  std::shared_ptr<arrow::dataset::ScanContext> context =
      std::make_shared<arrow::dataset::ScanContext>();
  std::shared_ptr<arrow::dataset::Dataset> dataset =
      RetrieveNativeInstance<arrow::dataset::Dataset>(dataset_id);
  std::shared_ptr<arrow::dataset::ScannerBuilder> scanner_builder =
      JniGetOrThrow(dataset->NewScan());

  std::vector<std::string> column_vector = ToStringVector(env, columns);
  JniAssertOkOrThrow(scanner_builder->Project(column_vector));
  JniAssertOkOrThrow(scanner_builder->BatchSize(batch_size));

  // initialize filters
  jsize exprs_len = env->GetArrayLength(filter);
  jbyte* exprs_bytes = env->GetByteArrayElements(filter, nullptr);
  arrow::dataset::types::Condition condition;
  if (!ParseProtobuf(reinterpret_cast<uint8_t*>(exprs_bytes), exprs_len, &condition)) {
    releaseFilterInput(filter, exprs_bytes, env);
    std::string error_message = "bad protobuf message";
    JniThrow(error_message);
  }
  if (condition.has_root()) {
    std::shared_ptr<arrow::dataset::Expression> translated_filter =
        JniGetOrThrow(TranslateFilter(condition));
    JniAssertOkOrThrow(scanner_builder->Filter(translated_filter));
  }
  auto scanner = JniGetOrThrow(scanner_builder->Finish());
  std::shared_ptr<DisposableScannerAdaptor> scanner_adaptor =
      JniGetOrThrow(DisposableScannerAdaptor::Create(scanner));
  jlong id = CreateNativeRef(scanner_adaptor);
  releaseFilterInput(filter, exprs_bytes, env);
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
 * Method:    makeSingleFileDatasetFactory
 * Signature: (Ljava/lang/String;II)J
 */
JNIEXPORT jlong JNICALL
Java_org_apache_arrow_dataset_file_JniWrapper_makeSingleFileDatasetFactory(
    JNIEnv* env, jobject, jstring path, jint file_format_id, jint file_system_id) {
  JNI_METHOD_START
  std::shared_ptr<arrow::dataset::FileFormat> file_format =
      JniGetOrThrow(GetFileFormat(file_format_id));
  std::string out_path;
  std::shared_ptr<arrow::fs::FileSystem> fs = JniGetOrThrow(
      GetFileSystem(file_system_id, JStringToCString(env, path), &out_path));
  std::shared_ptr<arrow::dataset::DatasetFactory> d = JniGetOrThrow(
      arrow::dataset::SingleFileDatasetFactory::Make(out_path, fs, file_format));
  return CreateNativeRef(d);
  JNI_METHOD_END(-1L)
}
