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

#include "arrow/dataset/file_base.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/filter.h"
#include "arrow/dataset/scanner.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/util/iterator.h"
#include "arrow/util/task_group.h"

namespace arrow {
namespace dataset {

Result<std::shared_ptr<arrow::io::RandomAccessFile>> FileSource::Open() const {
  if (id() == PATH) {
    return filesystem()->OpenInputFile(path());
  }

  return std::make_shared<::arrow::io::BufferReader>(buffer());
}

Result<std::shared_ptr<arrow::io::OutputStream>> FileSource::OpenWritable() const {
  if (!writable_) {
    return Status::Invalid("file source '", path(), "' is not writable");
  }

  if (id() == PATH) {
    return filesystem()->OpenOutputStream(path());
  }

  auto b = internal::checked_pointer_cast<ResizableBuffer>(buffer());
  return std::make_shared<::arrow::io::BufferOutputStream>(b);
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(FileSource source) {
  return MakeFragment(std::move(source), scalar(true));
}

Result<std::shared_ptr<FileFragment>> FileFormat::MakeFragment(
    FileSource source, std::shared_ptr<Expression> partition_expression) {
  return std::shared_ptr<FileFragment>(new FileFragment(
      std::move(source), shared_from_this(), std::move(partition_expression)));
}

Result<std::shared_ptr<WriteTask>> FileFormat::WriteFragment(
    FileSource destination, std::shared_ptr<Fragment> fragment,
    std::shared_ptr<ScanOptions> scan_options,
    std::shared_ptr<ScanContext> scan_context) {
  return Status::NotImplemented("writing fragment of format ", type_name());
}

Result<std::shared_ptr<Schema>> FileFragment::ReadPhysicalSchema() {
  return format_->Inspect(source_);
}

Result<ScanTaskIterator> FileFragment::Scan(std::shared_ptr<ScanOptions> options,
                                            std::shared_ptr<ScanContext> context) {
  return format_->ScanFile(source_, std::move(options), std::move(context));
}

FileSystemDataset::FileSystemDataset(std::shared_ptr<Schema> schema,
                                     std::shared_ptr<Expression> root_partition,
                                     std::shared_ptr<FileFormat> format,
                                     std::vector<std::shared_ptr<FileFragment>> fragments)
    : Dataset(std::move(schema), std::move(root_partition)),
      format_(std::move(format)),
      fragments_(std::move(fragments)) {}

Result<std::shared_ptr<FileSystemDataset>> FileSystemDataset::Make(
    std::shared_ptr<Schema> schema, std::shared_ptr<Expression> root_partition,
    std::shared_ptr<FileFormat> format,
    std::vector<std::shared_ptr<FileFragment>> fragments) {
  return std::shared_ptr<FileSystemDataset>(
      new FileSystemDataset(std::move(schema), std::move(root_partition),
                            std::move(format), std::move(fragments)));
}

Result<std::shared_ptr<Dataset>> FileSystemDataset::ReplaceSchema(
    std::shared_ptr<Schema> schema) const {
  RETURN_NOT_OK(CheckProjectable(*schema_, *schema));
  return std::shared_ptr<Dataset>(new FileSystemDataset(
      std::move(schema), partition_expression_, format_, fragments_));
}

std::vector<std::string> FileSystemDataset::files() const {
  std::vector<std::string> files;

  for (const auto& fragment : fragments_) {
    files.push_back(fragment->source().path());
  }

  return files;
}

std::string FileSystemDataset::ToString() const {
  std::string repr = "FileSystemDataset:";

  if (fragments_.empty()) {
    return repr + " []";
  }

  for (const auto& fragment : fragments_) {
    repr += "\n" + fragment->source().path();

    const auto& partition = fragment->partition_expression();
    if (!partition->Equals(true)) {
      repr += ": " + partition->ToString();
    }
  }

  return repr;
}

FragmentIterator FileSystemDataset::GetFragmentsImpl(
    std::shared_ptr<Expression> predicate) {
  FragmentVector fragments;

  for (const auto& fragment : fragments_) {
    if (predicate->IsSatisfiableWith(fragment->partition_expression())) {
      fragments.push_back(fragment);
    }
  }

  return MakeVectorIterator(std::move(fragments));
}

Result<std::shared_ptr<FileSystemDataset>> FileSystemDataset::Write(
    const WritePlan& plan, std::shared_ptr<ScanOptions> scan_options,
    std::shared_ptr<ScanContext> scan_context) {
  auto filesystem = plan.filesystem;
  if (filesystem == nullptr) {
    filesystem = std::make_shared<fs::LocalFileSystem>();
  }

  auto task_group = scan_context->TaskGroup();
  auto partition_base_dir = fs::internal::EnsureTrailingSlash(plan.partition_base_dir);
  auto extension = "." + plan.format->type_name();

  std::vector<std::shared_ptr<FileFragment>> fragments;
  for (size_t i = 0; i < plan.paths.size(); ++i) {
    const auto& op = plan.fragment_or_partition_expressions[i];
    if (util::holds_alternative<std::shared_ptr<Fragment>>(op)) {
      auto path = partition_base_dir + plan.paths[i] + extension;

      const auto& input_fragment = util::get<std::shared_ptr<Fragment>>(op);
      FileSource dest(path, filesystem);

      ARROW_ASSIGN_OR_RAISE(
          auto fragment,
          plan.format->MakeFragment(dest, input_fragment->partition_expression()));
      fragments.push_back(std::move(fragment));

      ARROW_ASSIGN_OR_RAISE(
          auto write_task,
          plan.format->WriteFragment(dest, input_fragment, scan_options, scan_context));
      task_group->Append([write_task] { return write_task->Execute(); });
    }
  }

  RETURN_NOT_OK(task_group->Finish());

  return Make(plan.schema, scalar(true), plan.format, fragments);
}

Status WriteTask::CreateDestinationParentDir() const {
  if (auto filesystem = destination_.filesystem()) {
    auto parent = fs::internal::GetAbstractPathParent(destination_.path()).first;
    return filesystem->CreateDir(parent, /* recursive = */ true);
  }

  return Status::OK();
}

Result<std::shared_ptr<SingleFileDataset>> SingleFileDataset::Make(
    std::shared_ptr<Schema> schema,
    std::shared_ptr<Expression> root_partition,
    std::string path,
    std::shared_ptr<fs::FileSystem> fs,
    std::shared_ptr<FileFormat> format) {
  std::shared_ptr<FileSource> file = std::make_shared<FileSource>(path, fs);
  return std::make_shared<SingleFileDataset>(schema, root_partition, file, fs, format);
}

Result<std::shared_ptr<SingleFileDataset>> SingleFileDataset::Make(
    std::shared_ptr<Schema> schema,
    std::shared_ptr<Expression> root_partition,
    std::shared_ptr<FileSource> file,
    std::shared_ptr<fs::FileSystem> fs,
    std::shared_ptr<FileFormat> format) {
  return std::make_shared<SingleFileDataset>(schema, root_partition, file, fs, format);
}

SingleFileDataset::SingleFileDataset(std::shared_ptr<Schema> schema,
                                     std::shared_ptr<Expression> root_partition,
                                     std::shared_ptr<FileSource> file,
                                     std::shared_ptr<fs::FileSystem> fs,
                                     std::shared_ptr<FileFormat> format)
    : Dataset(std::move(schema), std::move(root_partition)),
      file_(std::move(file)), fs_(std::move(fs)), format_(std::move(format)) {}

FragmentIterator SingleFileDataset::GetFragmentsImpl(
    std::shared_ptr<Expression> predicate) {
  std::vector<std::shared_ptr<FileSource>> files({file_});
  auto file_srcs_it = MakeVectorIterator(std::move(files));
  auto file_src_to_fragment = [this](const std::shared_ptr<FileSource>& source)
      -> Result<std::shared_ptr<Fragment>> {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Fragment> fragment,
                          format_->MakeFragment(*source))
    return fragment;
  };
  return MakeMaybeMapIterator(file_src_to_fragment, std::move(file_srcs_it));
}

Result<std::shared_ptr<Dataset>> SingleFileDataset::ReplaceSchema(
    std::shared_ptr<Schema> schema) const {
  RETURN_NOT_OK(CheckProjectable(*schema_, *schema));
  return std::shared_ptr<Dataset>(
      new SingleFileDataset(std::move(schema),
          partition_expression_, file_, fs_, format_));
}

}  // namespace dataset
}  // namespace arrow
