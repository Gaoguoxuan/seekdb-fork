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

#ifndef OCEANBASE_BASIC_OB_FUNCTION_TABLE_OP_H_
#define OCEANBASE_BASIC_OB_FUNCTION_TABLE_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/cmd/ob_load_data_file_reader.h"
#include "lib/charset/ob_charset.h"

namespace oceanbase
{
namespace sql
{

class ObExpr;
class ObFunctionTableSpec : public ObOpSpec
{
OB_UNIS_VERSION_V(5);
public:
  ObFunctionTableSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type), value_expr_(nullptr), column_exprs_(alloc),
      file_column_names_(), file_column_indexes_(alloc), has_correlated_expr_(false), is_file_sql_(false),
      is_file_sql_json_(false), is_file_sql_parquet_(false), is_file_sql_glob_(false), file_has_header_(true),
      file_sql_version_(0), file_field_delimiter_(','), file_quote_char_('"'),
      file_escape_char_('"'), file_null_string_(common::ObString::make_string("NULL"))
  {}
  ObExpr *value_expr_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> column_exprs_;
  common::ObSEArray<common::ObString, 16> file_column_names_;
  common::ObFixedArray<int64_t, common::ObIAllocator> file_column_indexes_;
  bool has_correlated_expr_;
  bool is_file_sql_;
  bool is_file_sql_json_;
  bool is_file_sql_parquet_;
  bool is_file_sql_glob_;
  bool file_has_header_;
  uint64_t file_sql_version_;
  char file_field_delimiter_;
  char file_quote_char_;
  char file_escape_char_;
  common::ObString file_null_string_;
};

class ObFunctionTableOp : public ObOperator
{
public:
  ObFunctionTableOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
    node_idx_(0),
    already_calc_(false),
    row_count_(0),
    col_count_(0),
    value_table_(NULL),
    file_reader_(nullptr),
    csv_reader_(),
    parquet_reader_(),
    csv_fields_(),
    json_row_allocator_(),
    glob_paths_(),
    glob_path_idx_(0),
    csv_row_buffer_(nullptr),
    file_path_(),
    file_header_consumed_(false),
    file_dev_(0),
    file_ino_(0),
    file_size_(0),
    file_mtime_(0),
    file_ctime_(0)
  {}

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  //virtual int inner_get_next_batch(int64_t max_row_cnt) override;
  virtual int inner_close() override;
  virtual void destroy() override;
private:
  int inner_get_next_row_udf();
  int inner_get_next_row_sys_func();
  int get_current_result(common::ObObj &result);
  int open_file_scan();
  int open_file_glob_scan();
  int get_next_file_sql_row();
  int get_next_file_json_row();
  int get_next_file_glob_row();
  int check_file_unchanged() const;
  int64_t node_idx_;
  bool already_calc_;
  int64_t row_count_;
  int64_t col_count_;
  common::ObObj value_;
  pl::ObPLCollection *value_table_;
  ObFileReader *file_reader_;
  ObFileSQLCSVReader csv_reader_;
  ObFileSQLParquetReader parquet_reader_;
  common::ObSEArray<common::ObString, 16> csv_fields_;
  common::ObArenaAllocator json_row_allocator_;
  common::ObSEArray<common::ObString, 16> glob_paths_;
  int64_t glob_path_idx_;
  char *csv_row_buffer_;
  common::ObString file_path_;
  bool file_header_consumed_;
  uint64_t file_dev_;
  uint64_t file_ino_;
  int64_t file_size_;
  int64_t file_mtime_;
  int64_t file_ctime_;
  int (ObFunctionTableOp::*next_row_func_)();
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_BASIC_OB_FUNCTION_TABLE_OP_H_ */
