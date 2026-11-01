/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_function_table_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_obj_cast_runtime.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_basic_session_info.h"
#include "common/json_type/ob_json_parse.h"
#include <limits.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <cstdio>
#ifndef _WIN32
#include <glob.h>
#endif


namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObFunctionTableSpec, ObOpSpec), value_expr_, column_exprs_, file_column_names_,
                    file_column_indexes_,
                    has_correlated_expr_, is_file_sql_, is_file_sql_json_, is_file_sql_parquet_, is_file_sql_glob_,
                    file_has_header_, file_sql_version_,
                    file_field_delimiter_, file_quote_char_, file_escape_char_, file_null_string_);

namespace
{

uint64_t file_sql_stat_signature(const struct stat &file_stat)
{
#ifdef __linux__
  const int64_t mtime_nsec = static_cast<int64_t>(file_stat.st_mtim.tv_nsec);
  const int64_t ctime_nsec = static_cast<int64_t>(file_stat.st_ctim.tv_nsec);
#else
  const int64_t mtime_nsec = 0;
  const int64_t ctime_nsec = 0;
#endif
  return ObFileSQLSchema::calc_file_signature(
      static_cast<uint64_t>(file_stat.st_dev),
      static_cast<uint64_t>(file_stat.st_ino),
      static_cast<int64_t>(file_stat.st_size),
      static_cast<int64_t>(file_stat.st_mtime), mtime_nsec,
      static_cast<int64_t>(file_stat.st_ctime), ctime_nsec);
}

int get_real_path(const ObString &path, char *buffer, const int64_t buffer_size, ObString &real_path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path.ptr()) || path.length() <= 0 || OB_ISNULL(buffer) || buffer_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    char *actual_path = nullptr;
#ifdef _WIN32
    actual_path = _fullpath(buffer, path.ptr(), static_cast<size_t>(buffer_size));
#else
    actual_path = realpath(path.ptr(), buffer);
#endif
    if (OB_ISNULL(actual_path)) {
      ret = OB_FILE_NOT_EXIST;
    } else {
      real_path.assign_ptr(actual_path, static_cast<int32_t>(strlen(actual_path)));
    }
  }
  return ret;
}
}

int ObFunctionTableOp::inner_open()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  already_calc_ = false;
  if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (MY_SPEC.is_file_sql_) {
    ret = MY_SPEC.is_file_sql_glob_ ? open_file_glob_scan() : open_file_scan();
  } else if (ObExtendType == MY_SPEC.value_expr_->datum_meta_.type_) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_udf;
  } else {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_sys_func;
  }
  return ret;
}

int ObFunctionTableOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else {
    node_idx_ = 0;
    if (MY_SPEC.is_file_sql_glob_) {
      glob_path_idx_ = 0;
    } else if (MY_SPEC.is_file_sql_) {
      file_header_consumed_ = false;
      ret = MY_SPEC.is_file_sql_parquet_ ? parquet_reader_.rewind() : csv_reader_.rewind();
    } else if (MY_SPEC.has_correlated_expr_) {
      row_count_ = 0;
      col_count_ = 0;
      value_table_ = NULL;
      already_calc_ = false;
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  row_count_ = 0;
  col_count_ = 0;
  value_table_ = NULL;
  if (OB_NOT_NULL(file_reader_)) {
    ObFileReader::destroy(file_reader_);
    file_reader_ = nullptr;
  }
  file_path_.reset();
  csv_row_buffer_ = nullptr;
  csv_fields_.reset();
  json_row_allocator_.reuse();
  glob_paths_.reset();
  glob_path_idx_ = 0;
  file_header_consumed_ = false;
  return ret;
}

void ObFunctionTableOp::destroy()
{
  ObOperator::destroy();
}


int ObFunctionTableOp::get_current_result(ObObj &result)
{
  int ret = OB_SUCCESS;
  void *data = NULL;
  CK (already_calc_);
  if (node_idx_ < 0 || node_idx_ >= row_count_ * col_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get current result in table function",
              K(node_idx_), K(row_count_), K(col_count_));
  }
  do {
    CK (node_idx_ >= 0);
    if (OB_SUCC(ret) && node_idx_ >= row_count_) {
      ret = OB_ITER_END;
    }
    CK (OB_NOT_NULL(value_table_));
    OX (data = value_table_->get_data());
    CK (OB_NOT_NULL(data));
    OX (result = (static_cast<ObObj*>(data))[node_idx_++]);
  } while (OB_SUCC(ret) && result.get_meta().get_type() == ObMaxType);
  return ret;
}

int ObFunctionTableOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.is_file_sql_glob_) {
    ret = get_next_file_glob_row();
  } else if (MY_SPEC.is_file_sql_json_) {
    ret = get_next_file_json_row();
  } else if (MY_SPEC.is_file_sql_) {
    ret = get_next_file_sql_row();
  } else {
    ret = (this->*next_row_func_)();
  }
  return ret;
}

int ObFunctionTableOp::open_file_glob_scan()
{
  int ret = OB_SUCCESS;
  glob_paths_.reset();
  glob_path_idx_ = 0;
#ifdef _WIN32
  ret = OB_NOT_SUPPORTED;
#else
  ObDatum *pattern_datum = nullptr;
  ObString pattern;
  ObString secure_file_priv;
  char pattern_buffer[PATH_MAX + 1];
  char scan_dir_buffer[PATH_MAX + 1];
  char canonical_scan_dir_buffer[PATH_MAX + 1];
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx_);
  glob_t matches;
  MEMSET(&matches, 0, sizeof(matches));
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, pattern_datum))) {
  } else if (OB_ISNULL(pattern_datum) || pattern_datum->is_null()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    pattern = pattern_datum->get_string();
    if (pattern.empty() || pattern.length() >= sizeof(pattern_buffer)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      MEMCPY(pattern_buffer, pattern.ptr(), pattern.length());
      pattern_buffer[pattern.length()] = '\0';
    }
  }
  if (OB_SUCC(ret)) {
    const char *wildcard = strpbrk(pattern_buffer, "*?[");
    const char *path_end = OB_NOT_NULL(wildcard) ? wildcard : pattern_buffer + strlen(pattern_buffer);
    const char *last_slash = nullptr;
    for (const char *p = pattern_buffer; p < path_end; ++p) {
      if ('/' == *p) {
        last_slash = p;
      }
    }
    int32_t scan_dir_len = 0;
    if (OB_ISNULL(last_slash)) {
      MEMCPY(scan_dir_buffer, ".", 2);
      scan_dir_len = 1;
    } else if (last_slash == pattern_buffer) {
      MEMCPY(scan_dir_buffer, "/", 2);
      scan_dir_len = 1;
    } else {
      scan_dir_len = static_cast<int32_t>(last_slash - pattern_buffer);
      MEMCPY(scan_dir_buffer, pattern_buffer, scan_dir_len);
      scan_dir_buffer[scan_dir_len] = '\0';
    }
    ObString scan_dir(scan_dir_len, scan_dir_buffer);
    ObString canonical_scan_dir;
    if (OB_FAIL(session->get_secure_file_priv(secure_file_priv))) {
    } else if (OB_FAIL(get_real_path(scan_dir, canonical_scan_dir_buffer,
                                     sizeof(canonical_scan_dir_buffer), canonical_scan_dir))) {
      ret = OB_ERR_NO_PRIVILEGE;
    } else if (OB_FAIL(ObResolverUtils::check_secure_path(secure_file_priv, canonical_scan_dir))) {
    }
  }
  if (OB_SUCC(ret)) {
    int glob_flags = 0;
#ifdef GLOB_LIMIT
    glob_flags |= GLOB_LIMIT;
#endif
    int glob_ret = glob(pattern_buffer, glob_flags, nullptr, &matches);
    if (GLOB_NOMATCH == glob_ret) {
      ret = OB_SUCCESS;
    } else if (0 != glob_ret) {
      ret = OB_ERR_SYS;
      LOG_WARN("failed to expand File SQL glob pattern", K(pattern), K(glob_ret), K(ret));
    } else {
      const int64_t max_matches = 10000;
      for (size_t i = 0; OB_SUCC(ret) && i < matches.gl_pathc; ++i) {
        const char *matched_path = matches.gl_pathv[i];
        char real_path_buffer[PATH_MAX + 1];
        struct stat file_stat;
        ObString real_path;
        if (OB_ISNULL(matched_path)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(get_real_path(
                       ObString(static_cast<int32_t>(strlen(matched_path)), matched_path),
                       real_path_buffer, sizeof(real_path_buffer), real_path))) {
          // Files disappearing after glob expansion are treated as an invalid scan input.
          ret = OB_INVALID_DATA;
        } else if (OB_FAIL(ObResolverUtils::check_secure_path(secure_file_priv, real_path))) {
        } else if (OB_FAIL(stat(real_path_buffer, &file_stat))) {
          ret = OB_INVALID_DATA;
        } else if (!S_ISREG(file_stat.st_mode)) {
          // Directory matches are not files and are omitted from the result.
        } else if (glob_paths_.count() >= max_matches) {
          ret = OB_SIZE_OVERFLOW;
          LOG_USER_ERROR(OB_SIZE_OVERFLOW);
        } else {
          ObString owned_path;
          if (OB_FAIL(ob_write_string(ctx_.get_allocator(), real_path, owned_path, true))) {
          } else if (OB_FAIL(glob_paths_.push_back(owned_path))) {
          }
        }
      }
    }
  }
  globfree(&matches);
#endif
  return ret;
}

int ObFunctionTableOp::get_next_file_glob_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(ctx_.check_status())) {
  } else if (glob_path_idx_ >= glob_paths_.count()) {
    ret = OB_ITER_END;
  } else if (1 != MY_SPEC.column_exprs_.count()
             || OB_ISNULL(MY_SPEC.column_exprs_.at(0))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObExpr *column_expr = MY_SPEC.column_exprs_.at(0);
    ObObj path_obj;
    path_obj.set_varchar(glob_paths_.at(glob_path_idx_++));
    path_obj.set_collation_type(column_expr->obj_meta_.get_collation_type());
    path_obj.set_collation_level(CS_LEVEL_IMPLICIT);
    if (OB_FAIL(column_expr->locate_datum_for_write(eval_ctx_).from_obj(
                    path_obj, column_expr->obj_datum_map_))) {
    } else {
      column_expr->set_evaluated_projected(eval_ctx_);
    }
  }
  return ret;
}

int ObFunctionTableOp::open_file_scan()
{
  int ret = OB_SUCCESS;
  ObDatum *path_datum = nullptr;
  ObString input_path;
  ObString real_path;
  ObString secure_file_priv;
  char path_buffer[PATH_MAX + 1];
  char real_path_buffer[PATH_MAX + 1];
  char *path_copy = nullptr;
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx_);
  ObFileReadParam file_param;
  int64_t file_size = 0;
  struct stat file_stat;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, path_datum))) {
  } else if (OB_ISNULL(path_datum) || path_datum->is_null()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObString path_value = path_datum->get_string();
    const int64_t path_len = path_value.length();
    if (path_len <= 0 || path_len >= sizeof(path_buffer)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      MEMCPY(path_buffer, path_value.ptr(), path_len);
      path_buffer[path_len] = '\0';
      input_path.assign_ptr(path_buffer, static_cast<int32_t>(path_len));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_real_path(input_path, real_path_buffer, sizeof(real_path_buffer), real_path))) {
    } else if (OB_FAIL(session->get_secure_file_priv(secure_file_priv))) {
    } else if (OB_FAIL(ObResolverUtils::check_secure_path(secure_file_priv, real_path))) {
    } else if (OB_ISNULL(path_copy = static_cast<char *>(ctx_.get_allocator().alloc(real_path.length() + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(path_copy, real_path.ptr(), real_path.length());
      path_copy[real_path.length()] = '\0';
      file_path_.assign_ptr(path_copy, real_path.length());
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(stat(file_path_.ptr(), &file_stat))) {
      ret = OB_FILE_NOT_EXIST;
    } else if (!S_ISREG(file_stat.st_mode)) {
      ret = OB_NOT_SUPPORTED;
    }
  }
  if (OB_SUCC(ret)) {
    file_param.file_location_ = ObLoadFileLocation::SERVER_DISK;
    file_param.filename_ = file_path_;
    if (OB_FAIL(ObFileReader::open(file_param, ctx_.get_allocator(), file_reader_))) {
    } else if (OB_FAIL(stat(file_path_.ptr(), &file_stat))) {
      ret = OB_FILE_NOT_EXIST;
    } else if (!S_ISREG(file_stat.st_mode)) {
      ret = OB_NOT_SUPPORTED;
    } else if (OB_FAIL(file_reader_->get_file_size(file_size))) {
    } else if (file_size != static_cast<int64_t>(file_stat.st_size)) {
      ret = OB_INVALID_DATA;
    } else if (MY_SPEC.is_file_sql_parquet_) {
      if (OB_FAIL(parquet_reader_.init(*file_reader_, ctx_.get_allocator(),
                                       &MY_SPEC.file_column_indexes_))) {
      } else if (OB_FAIL(parquet_reader_.rewind())) {
      }
    } else if (OB_ISNULL(csv_row_buffer_ = static_cast<char *>(ctx_.get_allocator().alloc(
                                             ObFileSQLCSVReader::INITIAL_ROW_BUFFER_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(csv_reader_.init(*file_reader_, ctx_.get_allocator(), csv_row_buffer_,
                                         ObFileSQLCSVReader::INITIAL_ROW_BUFFER_SIZE,
                                         MY_SPEC.is_file_sql_json_ ? '\x02' : MY_SPEC.file_field_delimiter_,
                                         MY_SPEC.is_file_sql_json_ ? '\x01' : MY_SPEC.file_quote_char_,
                                         MY_SPEC.is_file_sql_json_ ? '\x01' : MY_SPEC.file_escape_char_))) {
    } else if (OB_FAIL(csv_reader_.rewind())) {
    }
    if (OB_SUCC(ret)) {
      file_dev_ = static_cast<uint64_t>(file_stat.st_dev);
      file_ino_ = static_cast<uint64_t>(file_stat.st_ino);
      file_size_ = static_cast<int64_t>(file_stat.st_size);
      file_mtime_ = static_cast<int64_t>(file_stat.st_mtime);
      file_ctime_ = static_cast<int64_t>(file_stat.st_ctime);
      if (file_sql_stat_signature(file_stat) != MY_SPEC.file_sql_version_) {
        ret = OB_INVALID_DATA;
      }
      file_header_consumed_ = false;
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(file_reader_)) {
    ObFileReader::destroy(file_reader_);
    file_reader_ = nullptr;
  }
  return ret;
}

int ObFunctionTableOp::check_file_unchanged() const
{
  int ret = OB_SUCCESS;
  struct stat current_stat;
  if (file_path_.empty()) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(stat(file_path_.ptr(), &current_stat))) {
    ret = OB_INVALID_DATA;
  } else if (file_dev_ != static_cast<uint64_t>(current_stat.st_dev)
             || file_ino_ != static_cast<uint64_t>(current_stat.st_ino)
             || file_size_ != static_cast<int64_t>(current_stat.st_size)
             || file_mtime_ != static_cast<int64_t>(current_stat.st_mtime)
             || file_ctime_ != static_cast<int64_t>(current_stat.st_ctime)
             || file_sql_stat_signature(current_stat) != MY_SPEC.file_sql_version_) {
    ret = OB_INVALID_DATA;
  }
  return ret;
}

int ObFunctionTableOp::get_next_file_sql_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(ctx_.check_status())) {
  } else if (OB_FAIL(check_file_unchanged())) {
  } else {
    bool row_ready = false;
    while (OB_SUCC(ret) && !row_ready) {
      if (OB_FAIL(MY_SPEC.is_file_sql_parquet_
                      ? parquet_reader_.next_row(csv_fields_)
                      : csv_reader_.next_row(csv_fields_))) {
      } else if (!file_header_consumed_ && MY_SPEC.file_has_header_) {
        file_header_consumed_ = true;
      } else {
        file_header_consumed_ = true;
        row_ready = true;
      }
    }
    if (OB_SUCC(ret) && row_ready) {
      if (MY_SPEC.column_exprs_.count() != MY_SPEC.file_column_indexes_.count()
          || (MY_SPEC.is_file_sql_parquet_
              && csv_fields_.count() != MY_SPEC.column_exprs_.count())) {
        ret = OB_INVALID_DATA;
      }
      ObSQLSessionInfo *session = GET_MY_SESSION(ctx_);
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
        for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_exprs_.count(); ++i) {
          ObExpr *dst_expr = MY_SPEC.column_exprs_.at(i);
          const int64_t field_idx = MY_SPEC.is_file_sql_parquet_
              ? i : MY_SPEC.file_column_indexes_.at(i);
          if (OB_ISNULL(dst_expr)) {
            ret = OB_ERR_UNEXPECTED;
          } else if (field_idx < 0 || field_idx >= csv_fields_.count()) {
            ret = OB_INVALID_DATA;
          } else if (csv_fields_.at(field_idx).empty()
                     || (!MY_SPEC.file_null_string_.empty()
                         && common::ObCharset::case_insensitive_equal(
                                csv_fields_.at(field_idx), MY_SPEC.file_null_string_))) {
            dst_expr->locate_datum_for_write(eval_ctx_).set_null();
            dst_expr->set_evaluated_projected(eval_ctx_);
          } else if (ObTinyIntType == dst_expr->obj_meta_.get_type()
                     && (csv_fields_.at(field_idx).case_compare(ObString("TRUE")) == 0
                         || csv_fields_.at(field_idx).case_compare(ObString("FALSE")) == 0)) {
            ObObj bool_obj;
            bool_obj.set_tinyint(csv_fields_.at(field_idx).case_compare(ObString("TRUE")) == 0 ? 1 : 0);
            if (OB_FAIL(dst_expr->locate_datum_for_write(eval_ctx_).from_obj(
                            bool_obj, dst_expr->obj_datum_map_))) {
            } else {
              dst_expr->set_evaluated_projected(eval_ctx_);
            }
          } else {
            ObObj src_obj;
            ObObj dst_obj;
            const ObObjType dst_type = dst_expr->obj_meta_.get_type();
            const ObString &field_value = csv_fields_.at(field_idx);
            const bool is_true = field_value.case_compare(ObString("TRUE")) == 0;
            const bool is_false = field_value.case_compare(ObString("FALSE")) == 0;
            if ((ObIntType == dst_type || ObDoubleType == dst_type) && (is_true || is_false)) {
              // Boolean values widen to numeric types when the sampled rows also contain numbers.
              src_obj.set_varchar(ObString(1, is_true ? "1" : "0"));
            } else {
              src_obj.set_varchar(field_value);
            }
            src_obj.set_collation_type(dst_expr->obj_meta_.get_collation_type());
            src_obj.set_collation_level(CS_LEVEL_IMPLICIT);
            ObAccuracy dst_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][dst_type];
            ObCastCtx cast_ctx(&ctx_.get_allocator(), &dtc_params, CM_COLUMN_CONVERT,
                               dst_expr->obj_meta_.get_collation_type(), &dst_accuracy);
            ObSqlObjCastRuntime cast_runtime(&ctx_);
            cast_ctx.runtime_ = &cast_runtime;
            if (OB_FAIL(ObObjCaster::to_type(dst_type, dst_expr->obj_meta_.get_collation_type(),
                                             cast_ctx, src_obj, dst_obj))) {
            } else if (OB_FAIL(dst_expr->locate_datum_for_write(eval_ctx_).from_obj(
                                   dst_obj, dst_expr->obj_datum_map_))) {
            } else {
              dst_expr->set_evaluated_projected(eval_ctx_);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::get_next_file_json_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(ctx_.check_status())) {
  } else if (OB_FAIL(check_file_unchanged())) {
  } else {
    bool row_ready = false;
    ObJsonObject *object = nullptr;
    while (OB_SUCC(ret) && !row_ready) {
      json_row_allocator_.reuse();
      if (OB_FAIL(csv_reader_.next_row(csv_fields_))) {
      } else if (csv_fields_.count() != 1) {
        ret = OB_INVALID_DATA;
      } else if (csv_fields_.at(0).empty()) {
        // Ignore blank lines between JSONL records.
      } else {
        ObJsonNode *root = nullptr;
        if (OB_FAIL(ObJsonParser::get_tree(&json_row_allocator_, csv_fields_.at(0), root,
                                           ObJsonParser::JSN_DEFAULT_FLAG))) {
        } else if (OB_ISNULL(root) || ObJsonNodeType::J_OBJECT != root->json_type()) {
          ret = OB_INVALID_DATA;
        } else {
          object = static_cast<ObJsonObject *>(root);
          row_ready = true;
        }
      }
    }
    if (OB_SUCC(ret) && row_ready) {
      ObSQLSessionInfo *session = GET_MY_SESSION(ctx_);
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (MY_SPEC.file_column_names_.count() != MY_SPEC.column_exprs_.count()) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
        for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.column_exprs_.count(); ++i) {
          ObExpr *dst_expr = MY_SPEC.column_exprs_.at(i);
          ObJsonNode *value = object->get_value(MY_SPEC.file_column_names_.at(i));
          if (OB_ISNULL(dst_expr)) {
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_ISNULL(value) || ObJsonNodeType::J_NULL == value->json_type()) {
            dst_expr->locate_datum_for_write(eval_ctx_).set_null();
            dst_expr->set_evaluated_projected(eval_ctx_);
          } else if (ObJsonNodeType::J_BOOLEAN == value->json_type()
                     && ObTinyIntType == dst_expr->obj_meta_.get_type()) {
            ObObj bool_obj;
            bool_obj.set_tinyint(static_cast<ObJsonBoolean *>(value)->value() ? 1 : 0);
            if (OB_FAIL(dst_expr->locate_datum_for_write(eval_ctx_).from_obj(
                            bool_obj, dst_expr->obj_datum_map_))) {
            } else {
              dst_expr->set_evaluated_projected(eval_ctx_);
            }
          } else if (ObJsonNodeType::J_OBJECT == value->json_type()
                     || ObJsonNodeType::J_ARRAY == value->json_type()) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED,
                           "nested JSON objects and arrays are not supported in File SQL");
          } else {
            ObJsonBuffer value_buffer(&json_row_allocator_);
            ObObj src_obj;
            ObObj dst_obj;
            const ObObjType dst_type = dst_expr->obj_meta_.get_type();
            if (ObJsonNodeType::J_BOOLEAN == value->json_type()
                && (ObIntType == dst_type || ObDoubleType == dst_type)) {
              // Keep JSON booleans castable after schema inference widens the column to numeric.
              const bool bool_value = static_cast<ObJsonBoolean *>(value)->value();
              src_obj.set_varchar(ObString(1, bool_value ? "1" : "0"));
            } else if (OB_FAIL(value->print(value_buffer, false))) {
            } else {
              ObString value_string(static_cast<int32_t>(value_buffer.length()), value_buffer.ptr());
              src_obj.set_varchar(value_string);
            }
            src_obj.set_collation_type(dst_expr->obj_meta_.get_collation_type());
            src_obj.set_collation_level(CS_LEVEL_IMPLICIT);
            ObAccuracy dst_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][dst_type];
            ObCastCtx cast_ctx(&ctx_.get_allocator(), &dtc_params, CM_COLUMN_CONVERT,
                               dst_expr->obj_meta_.get_collation_type(), &dst_accuracy);
            ObSqlObjCastRuntime cast_runtime(&ctx_);
            cast_ctx.runtime_ = &cast_runtime;
            if (OB_SUCC(ret) && OB_FAIL(ObObjCaster::to_type(dst_type, dst_expr->obj_meta_.get_collation_type(),
                                                             cast_ctx, src_obj, dst_obj))) {
            } else if (OB_SUCC(ret) && OB_FAIL(dst_expr->locate_datum_for_write(eval_ctx_).from_obj(
                                             dst_obj, dst_expr->obj_datum_map_))) {
            } else if (OB_SUCC(ret)) {
              dst_expr->set_evaluated_projected(eval_ctx_);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_udf()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObExtendType != MY_SPEC.value_expr_->datum_meta_.type_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (FALSE_IT(plan_ctx->set_autoinc_id_tmp(0))) {
  } else if (OB_FAIL(ctx_.check_status())) {
  } else {
    ObDatum *value = nullptr;
    if (!already_calc_) {
      if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
      } else if (value->is_null()) {
        //do nothing
      } else if (OB_ISNULL(value_table_ 
                 = reinterpret_cast<pl::ObPLCollection*>(value->get_ext()))) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        row_count_ = value_table_->is_inited() ? value_table_->get_count() : 0;
        col_count_ = value_table_->get_column_count();
      }
      already_calc_ = true;
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(node_idx_ >= row_count_)) {
      ret = OB_ITER_END;
    }
    CK (MY_SPEC.column_exprs_.count() >= col_count_);
    ObObj obj_stack[col_count_];
    if (OB_SUCC(ret)) {
      if (nullptr != value_table_ 
          && ObExtendType == value_table_->get_element_type().get_obj_type()) {
        pl::ObPLComposite *composite = NULL;
        pl::ObPLRecord *record = NULL;
        ObObj record_obj;
        OZ (get_current_result(record_obj));
        if (OB_FAIL(ret)) {
        } else if (ObUserDefinedSQLType == record_obj.get_type()) {
          obj_stack[0] = record_obj;   
        } else if (record_obj.is_pl_extend()) {
          CK (OB_NOT_NULL(composite = reinterpret_cast<pl::ObPLComposite*>(record_obj.get_ext())));
          if (OB_SUCC(ret)) {
            if (composite->is_record()) {
              OX (record = static_cast<pl::ObPLRecord*>(composite));
              CK (record->get_count() == col_count_);
              for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
                OZ (record->get_element(i, obj_stack[i]));
              }
            } else if (composite->is_collection()) {
              OX (obj_stack[0] = record_obj);
            } else {
              ret = OB_ERR_UNEXPECTED;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected here", K(ret), K(record_obj), K(record_obj.meta_));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
          if (OB_FAIL(get_current_result(obj_stack[i]))) {
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
        if (obj_stack[i].is_null()) {
          MY_SPEC.column_exprs_.at(i)->locate_datum_for_write(eval_ctx_).set_null();
        } else {
          const ObObjDatumMapType &datum_map = MY_SPEC.column_exprs_.at(i)->obj_datum_map_;
          ObExpr * const &expr = MY_SPEC.column_exprs_.at(i);
          ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
          if (OB_FAIL(datum.from_obj(obj_stack[i], datum_map))) {
          } else if (is_lob_storage(obj_stack[i].get_type()) &&
                     OB_FAIL(ob_adjust_lob_datum(get_exec_ctx(), obj_stack[i],
                                                 expr->obj_meta_, datum_map,
                                                 get_exec_ctx().get_allocator(), datum))) {
          }
        }
        if (OB_SUCC(ret)) {
          MY_SPEC.column_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
        }
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_sys_func()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  ObDatum *value = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ctx_.check_status())) {
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
    if (OB_ITER_END != ret) {
    }
  } else {
    MY_SPEC.column_exprs_.at(0)->locate_datum_for_write(eval_ctx_).set_datum(*value);
    MY_SPEC.column_exprs_.at(0)->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}


} // end namespace sql
} // end namespace oceanbase
