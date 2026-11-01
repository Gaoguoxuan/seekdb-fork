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

#include <algorithm>
#include <cstring>
#include <gtest/gtest.h>
#include "rpc/obmysql/ob_mysql_packet.h"
#include "sql/engine/cmd/ob_load_data_file_reader.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
class TestMemoryFileReader : public ObFileReader
{
public:
  TestMemoryFileReader(ObIAllocator &allocator, const ObString &content)
    : ObFileReader(allocator), content_(content), offset_(0) {}
  virtual ~TestMemoryFileReader() {}

  int read(char *buf, int64_t count, int64_t &read_size) override
  {
    int ret = OB_SUCCESS;
    read_size = 0;
    if (OB_ISNULL(buf) || count < 0) {
      ret = OB_INVALID_ARGUMENT;
    } else if (offset_ < content_.length()) {
      read_size = std::min(count, static_cast<int64_t>(content_.length()) - offset_);
      if (read_size > 0) {
        MEMCPY(buf, content_.ptr() + offset_, read_size);
        offset_ += read_size;
      }
    }
    return ret;
  }
  int get_file_size(int64_t &file_size) override
  {
    file_size = content_.length();
    return OB_SUCCESS;
  }
  int seek(int64_t offset) override
  {
    int ret = OB_SUCCESS;
    if (offset < 0 || offset > content_.length()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      offset_ = offset;
    }
    return ret;
  }
  bool seekable() const override { return true; }
  int64_t get_offset() const override { return offset_; }
  bool eof() const override { return offset_ >= content_.length(); }

private:
  ObString content_;
  int64_t offset_;
};

TEST(TestFileSQLCSVReader, multiline_escaped_quotes_crlf_and_rewind)
{
  ObArenaAllocator allocator;
  const char input[] = "1,\"hello,\nworld\",\"say \"\"hi\"\"\"\r\n2,Bob,\r\n";
  TestMemoryFileReader file_reader(allocator, ObString::make_string(input));
  ObFileSQLCSVReader reader;
  char row_buffer[128];
  ObSEArray<ObString, 8> fields;
  ASSERT_EQ(OB_SUCCESS, reader.init(file_reader, allocator, row_buffer, sizeof(row_buffer)));

  ASSERT_EQ(OB_SUCCESS, reader.next_row(fields));
  ASSERT_EQ(3, fields.count());
  EXPECT_EQ(ObString::make_string("1"), fields.at(0));
  EXPECT_EQ(ObString::make_string("hello,\nworld"), fields.at(1));
  EXPECT_EQ(ObString::make_string("say \"hi\""), fields.at(2));

  ASSERT_EQ(OB_SUCCESS, reader.next_row(fields));
  ASSERT_EQ(3, fields.count());
  EXPECT_EQ(ObString::make_string("2"), fields.at(0));
  EXPECT_EQ(ObString::make_string("Bob"), fields.at(1));
  EXPECT_EQ(ObString::make_string(""), fields.at(2));
  EXPECT_EQ(OB_ITER_END, reader.next_row(fields));

  ASSERT_EQ(OB_SUCCESS, reader.rewind());
  ASSERT_EQ(OB_SUCCESS, reader.next_row(fields));
  EXPECT_EQ(ObString::make_string("1"), fields.at(0));
}

TEST(TestFileSQLCSVReader, reports_malformed_and_overflow_records)
{
  ObArenaAllocator allocator;
  {
    const char input[] = "1,\"unterminated";
    TestMemoryFileReader file_reader(allocator, ObString::make_string(input));
    ObFileSQLCSVReader reader;
    char row_buffer[32];
    ObSEArray<ObString, 8> fields;
    ASSERT_EQ(OB_SUCCESS, reader.init(file_reader, allocator, row_buffer, sizeof(row_buffer)));
    EXPECT_EQ(OB_INVALID_DATA, reader.next_row(fields));
  }
  {
    const char input[] = "12345\n";
    TestMemoryFileReader file_reader(allocator, ObString::make_string(input));
    ObFileSQLCSVReader reader;
    char row_buffer[4];
    ObSEArray<ObString, 8> fields;
    ASSERT_EQ(OB_SUCCESS, reader.init(file_reader, allocator, row_buffer, sizeof(row_buffer)));
    EXPECT_EQ(OB_BUF_NOT_ENOUGH, reader.next_row(fields));
  }
}

TEST(TestFileSQLSchema, infers_types_and_widens_deterministically)
{
  EXPECT_EQ(OB_FILE_SQL_NULL, ObFileSQLSchema::infer_value_type(ObString::make_string("")));
  EXPECT_EQ(OB_FILE_SQL_NULL, ObFileSQLSchema::infer_value_type(ObString::make_string(" NULL ")));
  EXPECT_EQ(OB_FILE_SQL_BIGINT, ObFileSQLSchema::infer_value_type(ObString::make_string("-9223372036854775808")));
  EXPECT_EQ(OB_FILE_SQL_DOUBLE, ObFileSQLSchema::infer_value_type(ObString::make_string("9223372036854775808")));
  EXPECT_EQ(OB_FILE_SQL_DOUBLE, ObFileSQLSchema::infer_value_type(ObString::make_string("1.25e-2")));
  EXPECT_EQ(OB_FILE_SQL_VARCHAR, ObFileSQLSchema::infer_value_type(ObString::make_string("1e999")));
  EXPECT_EQ(OB_FILE_SQL_BOOLEAN, ObFileSQLSchema::infer_value_type(ObString::make_string("TrUe")));
  EXPECT_EQ(OB_FILE_SQL_DATE, ObFileSQLSchema::infer_value_type(ObString::make_string("2024-02-29")));
  EXPECT_EQ(OB_FILE_SQL_VARCHAR, ObFileSQLSchema::infer_value_type(ObString::make_string("2023-02-29")));
  EXPECT_EQ(OB_FILE_SQL_DATETIME,
            ObFileSQLSchema::infer_value_type(ObString::make_string("2024-02-29 23:59:59.123")));
  EXPECT_EQ(OB_FILE_SQL_DOUBLE,
            ObFileSQLSchema::merge_types(OB_FILE_SQL_BIGINT, OB_FILE_SQL_DOUBLE));
  EXPECT_EQ(OB_FILE_SQL_DATETIME,
            ObFileSQLSchema::merge_types(OB_FILE_SQL_DATE, OB_FILE_SQL_DATETIME));
  EXPECT_EQ(OB_FILE_SQL_VARCHAR,
            ObFileSQLSchema::merge_types(OB_FILE_SQL_BOOLEAN, OB_FILE_SQL_BIGINT));
  EXPECT_EQ(OB_FILE_SQL_NULL,
            ObFileSQLSchema::merge_types(OB_FILE_SQL_NULL, OB_FILE_SQL_NULL));
}

TEST(TestFileSQLSchema, infers_full_csv_schema_and_unique_header_names)
{
  ObArenaAllocator allocator;
  const char input[] = "id,id,name,when,empty\n"
                       "1,2,A,2024-02-29,\n"
                       "2,3.5,B,2024-02-29 12:00:00,NULL\n";
  TestMemoryFileReader file_reader(allocator, ObString::make_string(input));
  ObFileSQLCSVReader reader;
  char row_buffer[256];
  ObSEArray<ObString, 8> fields;
  ObFileSQLSchema schema;
  ASSERT_EQ(OB_SUCCESS, reader.init(file_reader, allocator, row_buffer, sizeof(row_buffer)));
  ASSERT_EQ(OB_SUCCESS, schema.infer_csv_schema(reader, allocator, true, fields));
  ASSERT_EQ(5, schema.get_column_names().count());
  ASSERT_EQ(5, schema.get_column_types().count());
  EXPECT_EQ(ObString::make_string("id"), schema.get_column_names().at(0));
  EXPECT_EQ(ObString::make_string("id__2"), schema.get_column_names().at(1));
  EXPECT_EQ(OB_FILE_SQL_BIGINT, schema.get_column_types().at(0));
  EXPECT_EQ(OB_FILE_SQL_DOUBLE, schema.get_column_types().at(1));
  EXPECT_EQ(OB_FILE_SQL_VARCHAR, schema.get_column_types().at(2));
  EXPECT_EQ(OB_FILE_SQL_DATETIME, schema.get_column_types().at(3));
  EXPECT_EQ(OB_FILE_SQL_NULL, schema.get_column_types().at(4));
  // Inference rewinds seekable inputs so the execution scan can read again.
  ASSERT_EQ(OB_SUCCESS, reader.next_row(fields));
  EXPECT_EQ(ObString::make_string("id"), fields.at(0));
}

TEST(TestFileSQLSchema, generates_names_without_header_and_rejects_extra_columns)
{
  ObArenaAllocator allocator;
  {
    const char input[] = "1,true\n2,false\n";
    TestMemoryFileReader file_reader(allocator, ObString::make_string(input));
    ObFileSQLCSVReader reader;
    char row_buffer[64];
    ObSEArray<ObString, 8> fields;
    ObFileSQLSchema schema;
    ASSERT_EQ(OB_SUCCESS, reader.init(file_reader, allocator, row_buffer, sizeof(row_buffer)));
    ASSERT_EQ(OB_SUCCESS, schema.infer_csv_schema(reader, allocator, false, fields));
    ASSERT_EQ(2, schema.get_column_names().count());
    EXPECT_EQ(ObString::make_string("column0"), schema.get_column_names().at(0));
    EXPECT_EQ(ObString::make_string("column1"), schema.get_column_names().at(1));
    EXPECT_EQ(OB_FILE_SQL_BIGINT, schema.get_column_types().at(0));
    EXPECT_EQ(OB_FILE_SQL_BOOLEAN, schema.get_column_types().at(1));
  }
  {
    const char input[] = "a,b\n1,2,3\n";
    TestMemoryFileReader file_reader(allocator, ObString::make_string(input));
    ObFileSQLCSVReader reader;
    char row_buffer[64];
    ObSEArray<ObString, 8> fields;
    ObFileSQLSchema schema;
    ASSERT_EQ(OB_SUCCESS, reader.init(file_reader, allocator, row_buffer, sizeof(row_buffer)));
    EXPECT_EQ(OB_INVALID_DATA, schema.infer_csv_schema(reader, allocator, true, fields));
  }
}

TEST(TestFileSQLSchema, honors_csv_delimiter_quote_escape_and_null_string)
{
  ObArenaAllocator allocator;
  const char input[] = "id;value;empty\n1;'alpha;beta';N/A\n2;gamma;N/A\n";
  TestMemoryFileReader file_reader(allocator, ObString::make_string(input));
  ObFileSQLCSVReader reader;
  char row_buffer[128];
  ObSEArray<ObString, 8> fields;
  ObFileSQLSchema schema;
  ASSERT_EQ(OB_SUCCESS,
            reader.init(file_reader, allocator, row_buffer, sizeof(row_buffer), ';', '\'', '\\'));
  ASSERT_EQ(OB_SUCCESS,
            schema.infer_csv_schema(reader, allocator, true, fields, ObString::make_string("N/A")));
  ASSERT_EQ(3, schema.get_column_names().count());
  EXPECT_EQ(OB_FILE_SQL_BIGINT, schema.get_column_types().at(0));
  EXPECT_EQ(OB_FILE_SQL_VARCHAR, schema.get_column_types().at(1));
  EXPECT_EQ(OB_FILE_SQL_NULL, schema.get_column_types().at(2));

  ASSERT_EQ(OB_SUCCESS, reader.next_row(fields));
  ASSERT_EQ(3, fields.count());
  EXPECT_EQ(ObString::make_string("id"), fields.at(0));
  ASSERT_EQ(OB_SUCCESS, reader.next_row(fields));
  EXPECT_EQ(ObString::make_string("alpha;beta"), fields.at(1));
  ASSERT_EQ(OB_SUCCESS, reader.next_row(fields));
  EXPECT_EQ(ObString::make_string("N/A"), fields.at(2));
}
} // namespace sql
} // namespace oceanbase
