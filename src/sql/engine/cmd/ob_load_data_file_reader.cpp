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

#define USING_LOG_PREFIX  SQL_ENG

#include "sql/engine/cmd/ob_load_data_file_reader.h"
#include "lib/charset/ob_charset.h"
#include "rpc/obmysql/ob_i_cs_mem_pool.h"
#include "rpc/obmysql/packet/ompk_local_infile.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/compress/zstd_1_3_8/ob_zstd_wrapper.h"
#include "lib/compress/ob_compress_util.h"
#include "sql/engine/ob_exec_context.h"
#include "common/json_type/ob_json_parse.h"
#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace oceanbase
{
namespace sql
{

const lib::ObLabel MEMORY_LABEL = lib::ObLabel("LoadDataReader");

#define MEMORY_ATTR ObMemAttr(MEMORY_LABEL)

/**
 * ObFileReadParam
 */

ObFileReadParam::ObFileReadParam()
    : compression_format_(ObCSVGeneralFormat::ObCSVCompression::NONE),
      packet_handle_(NULL),
      session_(NULL),
      timeout_ts_(-1)
{
}

int ObFileReadParam::parse_compression_format(ObString compression_name,
                                              ObString filename,
                                              ObCSVGeneralFormat::ObCSVCompression &compression_format)
{
  int ret = OB_SUCCESS;
  if (compression_name.length() == 0) {
    compression_format = ObCSVGeneralFormat::ObCSVCompression::NONE;
  } else if (OB_FAIL(compression_algorithm_from_string(compression_name, compression_format))) {
  } else if (ObCSVGeneralFormat::ObCSVCompression::AUTO == compression_format) {
    ret = compression_algorithm_from_suffix(filename, compression_format);
  }
  return ret;
}

/**
 * ObFileReader
 */

int ObFileReader::open(const ObFileReadParam &param, ObIAllocator &allocator, ObFileReader *& file_reader)
{
  int ret = OB_SUCCESS;
  file_reader = nullptr;
  
  if (param.file_location_ == ObLoadFileLocation::SERVER_DISK) {
    ObRandomFileReader *tmp_reader = OB_NEW(ObRandomFileReader, MEMORY_ATTR, allocator);
    if (OB_ISNULL(tmp_reader)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(tmp_reader->open(param.filename_))) {
      OB_DELETE(ObRandomFileReader, MEMORY_ATTR, tmp_reader);
    } else {
      file_reader = tmp_reader;
    }
  } else if (param.file_location_ == ObLoadFileLocation::CLIENT_DISK) {
    if (OB_ISNULL(param.packet_handle_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ObPacketStreamFileReader *tmp_reader = OB_NEW(ObPacketStreamFileReader, MEMORY_ATTR, allocator);
      if (OB_ISNULL(tmp_reader)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(tmp_reader->open(param.filename_, *param.packet_handle_, param.session_, param.timeout_ts_))) {
        OB_DELETE(ObPacketStreamFileReader, MEMORY_ATTR, tmp_reader);
      } else {
        file_reader = tmp_reader;
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
  }

  if (OB_SUCC(ret)) {
    ObFileReader *decompress_reader = nullptr;
    ret = open_decompress_reader(param, allocator, file_reader, decompress_reader);
    if (OB_SUCC(ret) && OB_NOT_NULL(decompress_reader)) {
      file_reader = decompress_reader;
    }
  }

  return ret;
}

void ObFileReader::destroy(ObFileReader *file_reader)
{
  if (OB_NOT_NULL(file_reader)) {
    OB_DELETE(ObFileReader, MEMORY_ATTR, file_reader);
  }
}

int ObFileReader::open_decompress_reader(const ObFileReadParam &param,
                                         ObIAllocator &allocator,
                                         ObFileReader *source_reader,
                                         ObFileReader *&file_reader)
{
  int ret = OB_SUCCESS;
  if (param.compression_format_ == ObCSVGeneralFormat::ObCSVCompression::NONE) {
    file_reader = source_reader;
  } else {
    ObDecompressFileReader *tmp_reader = OB_NEW(ObDecompressFileReader, MEMORY_ATTR, allocator);
    if (OB_ISNULL(tmp_reader)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(tmp_reader->open(param, source_reader))) {
      LOG_WARN("failed to open decompress file reader");
      OB_DELETE(ObDecompressFileReader, MEMORY_ATTR, tmp_reader);
    } else {
      file_reader = tmp_reader;
    }
  }
  return ret;
}

int ObFileReader::readn(char *buffer, int64_t count, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;
  while (OB_SUCC(ret) && !eof() && read_size < count) {
    int64_t this_read_size = 0;
    ret = this->read(buffer + read_size, count - read_size, this_read_size);
    if (OB_SUCC(ret)) {
      read_size += this_read_size;
    }
  }
  return ret;
}

ObFileSQLCSVReader::ObFileSQLCSVReader()
  : file_reader_(nullptr),
    allocator_(nullptr),
    input_buffer_(nullptr),
    input_pos_(0),
    input_len_(0),
    row_buffer_(nullptr),
    row_buffer_size_(0),
    field_delimiter_(','),
    quote_char_('"'),
    escape_char_('"'),
    input_eof_(false),
    is_inited_(false)
{}

ObFileSQLCSVReader::~ObFileSQLCSVReader()
{}

int ObFileSQLCSVReader::init(ObFileReader &file_reader,
                             ObIAllocator &allocator,
                             char *row_buffer,
                             const int64_t row_buffer_size,
                             const char field_delimiter,
                             const char quote_char,
                             const char escape_char)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
      } else if (OB_ISNULL(row_buffer) || row_buffer_size <= 0 || row_buffer_size > INT32_MAX
             || row_buffer_size > MAX_ROW_BUFFER_SIZE
                 || field_delimiter == '\n'
             || field_delimiter == '\r' || field_delimiter == quote_char
             || quote_char == '\n' || quote_char == '\r'
             || escape_char == '\n' || escape_char == '\r') {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(input_buffer_ = static_cast<char *>(allocator.alloc(INPUT_BUFFER_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    file_reader_ = &file_reader;
    allocator_ = &allocator;
    row_buffer_ = row_buffer;
    row_buffer_size_ = row_buffer_size;
    field_delimiter_ = field_delimiter;
    quote_char_ = quote_char;
    escape_char_ = escape_char;
    is_inited_ = true;
  }
  return ret;
}

void ObFileSQLCSVReader::reuse()
{
  input_pos_ = 0;
  input_len_ = 0;
  input_eof_ = false;
}

int ObFileSQLCSVReader::rewind()
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || OB_ISNULL(file_reader_)) {
    ret = OB_NOT_INIT;
  } else if (!file_reader_->seekable()) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(file_reader_->seek(0))) {
  } else {
    reuse();
  }
  return ret;
}

int ObFileSQLCSVReader::read_byte(char &c, bool &is_eof)
{
  int ret = OB_SUCCESS;
  is_eof = false;
  int64_t empty_read_count = 0;
  if (!is_inited_ || OB_ISNULL(file_reader_) || OB_ISNULL(input_buffer_)) {
    ret = OB_NOT_INIT;
  }
  while (OB_SUCC(ret) && input_pos_ >= input_len_ && !is_eof) {
    if (input_eof_ || file_reader_->eof()) {
      input_eof_ = true;
      is_eof = true;
    } else {
      input_pos_ = 0;
      input_len_ = 0;
      if (OB_FAIL(file_reader_->read(input_buffer_, INPUT_BUFFER_SIZE, input_len_))) {
      } else if (input_len_ < 0 || input_len_ > INPUT_BUFFER_SIZE) {
        ret = OB_ERR_UNEXPECTED;
      } else if (input_len_ == 0) {
        if (file_reader_->eof()) {
          input_eof_ = true;
          is_eof = true;
        } else if (++empty_read_count >= 16) {
          // Avoid spinning forever if a reader repeatedly returns no data and
          // does not report EOF.
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }
  }
  if (OB_SUCC(ret) && !is_eof) {
    c = input_buffer_[input_pos_++];
  }
  return ret;
}

int ObFileSQLCSVReader::ensure_record_capacity(const int64_t required_size,
                                               const int64_t record_len)
{
  int ret = OB_SUCCESS;
  if (required_size <= row_buffer_size_) {
  } else if (OB_ISNULL(allocator_) || required_size > MAX_ROW_BUFFER_SIZE) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    int64_t new_size = row_buffer_size_;
    while (new_size < required_size && new_size < MAX_ROW_BUFFER_SIZE) {
      new_size = std::min(new_size * 2, MAX_ROW_BUFFER_SIZE);
    }
    char *new_buffer = nullptr;
    if (OB_ISNULL(new_buffer = static_cast<char *>(allocator_->alloc(new_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      if (record_len > 0) {
        MEMCPY(new_buffer, row_buffer_, record_len);
      }
      row_buffer_ = new_buffer;
      row_buffer_size_ = new_size;
    }
  }
  return ret;
}

int ObFileSQLCSVReader::read_record(int64_t &record_len)
{
  int ret = OB_SUCCESS;
  record_len = 0;
  bool record_started = false;
  bool in_quotes = false;
  bool quote_pending = false;
  bool field_start = true;
  bool row_done = false;
  while (OB_SUCC(ret) && !row_done) {
    char c = '\0';
    bool is_eof = false;
    if (OB_FAIL(read_byte(c, is_eof))) {
    } else if (is_eof) {
      if (!record_started && record_len == 0) {
        ret = OB_ITER_END;
      } else if (in_quotes && !quote_pending) {
        ret = OB_INVALID_DATA;
      } else {
        row_done = true;
      }
    } else {
      record_started = true;
      if (quote_pending) {
        if (c == quote_char_) {
          if (OB_FAIL(ensure_record_capacity(record_len + 1, record_len))) {
          } else {
            row_buffer_[record_len++] = c;
            quote_pending = false;
            continue;
          }
        } else {
          quote_pending = false;
          in_quotes = false;
        }
      }
      if (OB_SUCC(ret)) {
        if (in_quotes) {
          if (escape_char_ != quote_char_ && escape_char_ != '\0' && c == escape_char_) {
            char escaped = '\0';
            bool escaped_eof = false;
            if (OB_FAIL(ensure_record_capacity(record_len + 1, record_len))) {
            } else {
              row_buffer_[record_len++] = c;
              if (OB_FAIL(read_byte(escaped, escaped_eof))) {
              } else if (escaped_eof) {
                ret = OB_INVALID_DATA;
              } else if (OB_FAIL(ensure_record_capacity(record_len + 1, record_len))) {
              } else {
                row_buffer_[record_len++] = escaped;
              }
            }
          } else if (c == quote_char_) {
            if (OB_FAIL(ensure_record_capacity(record_len + 1, record_len))) {
            } else {
              row_buffer_[record_len++] = c;
              quote_pending = true;
            }
          } else if (OB_FAIL(ensure_record_capacity(record_len + 1, record_len))) {
          } else {
            row_buffer_[record_len++] = c;
          }
        } else if (c == '\n') {
          row_done = true;
        } else {
          if (OB_FAIL(ensure_record_capacity(record_len + 1, record_len))) {
          } else {
            row_buffer_[record_len++] = c;
            if (c == quote_char_ && field_start) {
              in_quotes = true;
              field_start = false;
            } else {
              field_start = c == field_delimiter_;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObFileSQLCSVReader::parse_record(const int64_t record_len,
                                     ObIArray<ObString> &fields)
{
  int ret = OB_SUCCESS;
  fields.reset();
  int64_t len = record_len;
  if (len > 0 && row_buffer_[len - 1] == '\r') {
    --len;
  }
  int64_t pos = 0;
  int64_t write_pos = 0;
  bool more_fields = true;
  while (OB_SUCC(ret) && more_fields) {
    const int64_t field_begin = write_pos;
    if (pos < len && row_buffer_[pos] == quote_char_) {
      ++pos;
      bool closed = false;
      while (OB_SUCC(ret) && pos < len && !closed) {
        const char c = row_buffer_[pos];
        if (escape_char_ != quote_char_ && escape_char_ != '\0'
            && c == escape_char_ && pos + 1 < len) {
          row_buffer_[write_pos++] = row_buffer_[pos + 1];
          pos += 2;
        } else if (c == quote_char_) {
          if (escape_char_ == quote_char_ && pos + 1 < len
              && row_buffer_[pos + 1] == quote_char_) {
            row_buffer_[write_pos++] = quote_char_;
            pos += 2;
          } else {
            ++pos;
            closed = true;
          }
        } else {
          row_buffer_[write_pos++] = row_buffer_[pos++];
        }
      }
      if (!closed) {
        ret = OB_INVALID_DATA;
      } else if (pos < len && row_buffer_[pos] != field_delimiter_) {
        ret = OB_INVALID_DATA;
      }
    } else {
      while (OB_SUCC(ret) && pos < len && row_buffer_[pos] != field_delimiter_) {
        if (escape_char_ != quote_char_ && escape_char_ != '\0'
            && row_buffer_[pos] == escape_char_ && pos + 1 < len) {
          row_buffer_[write_pos++] = row_buffer_[pos + 1];
          pos += 2;
        } else {
          row_buffer_[write_pos++] = row_buffer_[pos++];
        }
      }
    }
    if (OB_SUCC(ret)) {
      ret = fields.push_back(ObString(static_cast<int32_t>(write_pos - field_begin),
                                      row_buffer_ + field_begin));
      if (OB_SUCC(ret) && pos < len) {
        if (row_buffer_[pos] != field_delimiter_) {
          ret = OB_INVALID_DATA;
        } else {
          ++pos;
        }
      } else if (OB_SUCC(ret)) {
        more_fields = false;
      }
    }
  }
  return ret;
}

int ObFileSQLCSVReader::next_row(ObIArray<ObString> &fields)
{
  int ret = OB_SUCCESS;
  int64_t record_len = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(read_record(record_len))) {
  } else if (OB_FAIL(parse_record(record_len, fields))) {
  }
  return ret;
}

namespace
{
bool file_sql_is_space(const char c)
{
  return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f' || c == '\v';
}

char file_sql_ascii_lower(const char c)
{
  return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
}

bool file_sql_equals_ci(const ObString &value, const char *literal)
{
  const int64_t len = static_cast<int64_t>(strlen(literal));
  bool equal = value.length() == len;
  for (int64_t i = 0; equal && i < len; ++i) {
    equal = file_sql_ascii_lower(value.ptr()[i]) == file_sql_ascii_lower(literal[i]);
  }
  return equal;
}

bool file_sql_is_valid_date(const char *ptr, const int64_t len)
{
  bool valid = len == 10 && ptr[4] == '-' && ptr[7] == '-';
  for (int64_t i = 0; valid && i < len; ++i) {
    if (i != 4 && i != 7) {
      valid = ptr[i] >= '0' && ptr[i] <= '9';
    }
  }
  if (valid) {
    const int year = (ptr[0] - '0') * 1000 + (ptr[1] - '0') * 100
                   + (ptr[2] - '0') * 10 + (ptr[3] - '0');
    const int month = (ptr[5] - '0') * 10 + (ptr[6] - '0');
    const int day = (ptr[8] - '0') * 10 + (ptr[9] - '0');
    static const int days_per_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    const bool leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    valid = year >= 1 && month >= 1 && month <= 12 && day >= 1
         && day <= days_per_month[month] + ((month == 2 && leap) ? 1 : 0);
  }
  return valid;
}

bool file_sql_is_valid_datetime(const char *ptr, const int64_t len)
{
  bool valid = (len == 19 || (len > 20 && len <= 26 && ptr[19] == '.'))
            && file_sql_is_valid_date(ptr, 10)
            && (ptr[10] == ' ' || ptr[10] == 'T' || ptr[10] == 't')
            && ptr[13] == ':' && ptr[16] == ':';
  for (int64_t i = 11; valid && i < 19; ++i) {
    if (i != 13 && i != 16) {
      valid = ptr[i] >= '0' && ptr[i] <= '9';
    }
  }
  for (int64_t i = 20; valid && i < len; ++i) {
    valid = ptr[i] >= '0' && ptr[i] <= '9';
  }
  if (valid) {
    const int hour = (ptr[11] - '0') * 10 + (ptr[12] - '0');
    const int minute = (ptr[14] - '0') * 10 + (ptr[15] - '0');
    const int second = (ptr[17] - '0') * 10 + (ptr[18] - '0');
    valid = hour <= 23 && minute <= 59 && second <= 59;
  }
  return valid;
}

bool file_sql_is_numeric(const ObString &value)
{
  bool numeric = false;
  int64_t pos = 0;
  if (pos < value.length() && (value.ptr()[pos] == '+' || value.ptr()[pos] == '-')) { ++pos; }
  int64_t digit_count = 0;
  while (pos < value.length() && value.ptr()[pos] >= '0' && value.ptr()[pos] <= '9') {
    ++pos;
    ++digit_count;
  }
  if (pos < value.length() && value.ptr()[pos] == '.') {
    ++pos;
    while (pos < value.length() && value.ptr()[pos] >= '0' && value.ptr()[pos] <= '9') {
      ++pos;
      ++digit_count;
    }
  }
  bool valid = digit_count > 0;
  if (valid && pos < value.length() && (value.ptr()[pos] == 'e' || value.ptr()[pos] == 'E')) {
    ++pos;
    if (pos < value.length() && (value.ptr()[pos] == '+' || value.ptr()[pos] == '-')) { ++pos; }
    const int64_t exponent_begin = pos;
    while (pos < value.length() && value.ptr()[pos] >= '0' && value.ptr()[pos] <= '9') { ++pos; }
    valid = pos > exponent_begin;
  }
  valid = valid && pos == value.length();
  // strtod is only used after checking the complete numeric grammar. Keep the
  // temporary bounded so a very wide CSV field cannot consume stack space.
  if (valid && value.length() > 0 && value.length() < 128) {
    char text[128];
    MEMCPY(text, value.ptr(), value.length());
    text[value.length()] = '\0';
    errno = 0;
    char *end = nullptr;
    const double number = strtod(text, &end);
    numeric = end == text + value.length() && errno != ERANGE && std::isfinite(number);
  }
  return numeric;
}
} // namespace

ObFileSQLType ObFileSQLSchema::infer_value_type(const ObString &value,
                                               const ObString &null_string)
{
  ObFileSQLType type = OB_FILE_SQL_VARCHAR;
  int64_t begin = 0;
  int64_t end = value.length();
  while (begin < end && file_sql_is_space(value.ptr()[begin])) { ++begin; }
  while (end > begin && file_sql_is_space(value.ptr()[end - 1])) { --end; }
  if (begin == end) {
    type = OB_FILE_SQL_NULL;
  } else {
    const ObString trimmed(static_cast<int32_t>(end - begin), value.ptr() + begin);
    if ((!null_string.empty() && common::ObCharset::case_insensitive_equal(trimmed, null_string))
        || (null_string.empty() && file_sql_equals_ci(trimmed, "NULL"))) {
      type = OB_FILE_SQL_NULL;
    } else if (file_sql_equals_ci(trimmed, "TRUE") || file_sql_equals_ci(trimmed, "FALSE")
               || (1 == trimmed.length() && ('0' == trimmed.ptr()[0] || '1' == trimmed.ptr()[0]))) {
      type = OB_FILE_SQL_BOOLEAN;
    } else if (file_sql_is_valid_datetime(trimmed.ptr(), trimmed.length())) {
      type = OB_FILE_SQL_DATETIME;
    } else if (file_sql_is_valid_date(trimmed.ptr(), trimmed.length())) {
      type = OB_FILE_SQL_DATE;
    } else {
      bool integer_syntax = true;
      int64_t pos = 0;
      bool negative = false;
      if (trimmed.ptr()[pos] == '+' || trimmed.ptr()[pos] == '-') {
        negative = trimmed.ptr()[pos] == '-';
        ++pos;
      }
      const int64_t digit_begin = pos;
      uint64_t magnitude = 0;
      const uint64_t limit = negative ? (static_cast<uint64_t>(INT64_MAX) + 1)
                                      : static_cast<uint64_t>(INT64_MAX);
      bool in_range = true;
      for (; pos < trimmed.length(); ++pos) {
        const char c = trimmed.ptr()[pos];
        if (c < '0' || c > '9') {
          integer_syntax = false;
          break;
        }
        const uint64_t digit = static_cast<uint64_t>(c - '0');
        if (magnitude > (limit - digit) / 10) {
          in_range = false;
        } else if (in_range) {
          magnitude = magnitude * 10 + digit;
        }
      }
      integer_syntax = integer_syntax && pos > digit_begin;
      if (integer_syntax && in_range) {
        type = OB_FILE_SQL_BIGINT;
      } else if (file_sql_is_numeric(trimmed)) {
        type = OB_FILE_SQL_DOUBLE;
      }
    }
  }
  return type;
}

ObFileSQLType ObFileSQLSchema::merge_types(const ObFileSQLType left, const ObFileSQLType right)
{
  ObFileSQLType merged = left;
  if (left == OB_FILE_SQL_NULL) {
    merged = right;
  } else if (right == OB_FILE_SQL_NULL || left == right) {
    merged = left;
  } else if ((left == OB_FILE_SQL_BIGINT && right == OB_FILE_SQL_DOUBLE)
             || (left == OB_FILE_SQL_DOUBLE && right == OB_FILE_SQL_BIGINT)) {
    merged = OB_FILE_SQL_DOUBLE;
  } else if ((left == OB_FILE_SQL_BOOLEAN && right == OB_FILE_SQL_BIGINT)
             || (left == OB_FILE_SQL_BIGINT && right == OB_FILE_SQL_BOOLEAN)) {
    merged = OB_FILE_SQL_BIGINT;
  } else if ((left == OB_FILE_SQL_BOOLEAN && right == OB_FILE_SQL_DOUBLE)
             || (left == OB_FILE_SQL_DOUBLE && right == OB_FILE_SQL_BOOLEAN)) {
    merged = OB_FILE_SQL_DOUBLE;
  } else if ((left == OB_FILE_SQL_DATE && right == OB_FILE_SQL_DATETIME)
             || (left == OB_FILE_SQL_DATETIME && right == OB_FILE_SQL_DATE)) {
    merged = OB_FILE_SQL_DATETIME;
  } else {
    merged = OB_FILE_SQL_VARCHAR;
  }
  return merged;
}

uint64_t ObFileSQLSchema::calc_file_signature(const uint64_t dev,
                                             const uint64_t ino,
                                             const int64_t size,
                                             const int64_t mtime_sec,
                                             const int64_t mtime_nsec,
                                             const int64_t ctime_sec,
                                             const int64_t ctime_nsec)
{
  const uint64_t values[] = {dev, ino, static_cast<uint64_t>(size),
                             static_cast<uint64_t>(mtime_sec), static_cast<uint64_t>(mtime_nsec),
                             static_cast<uint64_t>(ctime_sec), static_cast<uint64_t>(ctime_nsec)};
  uint64_t signature = 1469598103934665603ULL;
  for (int64_t i = 0; i < ARRAYSIZEOF(values); ++i) {
    signature ^= values[i];
    signature *= 1099511628211ULL;
  }
  return signature;
}

void ObFileSQLSchema::reset()
{
  column_names_.reset();
  column_types_.reset();
  column_nullable_.reset();
}

int ObFileSQLSchema::make_column_name(ObIAllocator &allocator,
                                      const ObString &name,
                                      const int64_t suffix,
                                      ObString &column_name)
{
  int ret = OB_SUCCESS;
  char generated[64];
  const char *base = name.ptr();
  int64_t base_len = name.length();
  char suffix_text[32];
  int64_t suffix_len = 0;
  if (base_len == 0) {
    const int written = snprintf(generated, sizeof(generated), "column%ld", static_cast<long>(suffix));
    if (written < 0 || written >= static_cast<int>(sizeof(generated))) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      base = generated;
      base_len = written;
    }
  } else if (suffix > 1) {
    const int written = snprintf(suffix_text, sizeof(suffix_text), "__%ld", static_cast<long>(suffix));
    if (written < 0 || written >= static_cast<int>(sizeof(suffix_text))) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      suffix_len = written;
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t total_len = base_len + suffix_len;
    char *buffer = nullptr;
    if (total_len > INT32_MAX) {
      ret = OB_SIZE_OVERFLOW;
    } else if (OB_ISNULL(buffer = static_cast<char *>(allocator.alloc(total_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      if (base_len > 0) { MEMCPY(buffer, base, base_len); }
      if (suffix_len > 0) { MEMCPY(buffer + base_len, suffix_text, suffix_len); }
      column_name.assign_ptr(buffer, static_cast<int32_t>(total_len));
    }
  }
  return ret;
}

int ObFileSQLSchema::append_unique_column_name(ObIAllocator &allocator,
                                              const ObString &name,
                                              const int64_t column_idx)
{
  int ret = OB_SUCCESS;
  bool unique = false;
  int64_t suffix = 1;
  while (OB_SUCC(ret) && !unique) {
    ObString candidate;
    if (OB_FAIL(make_column_name(allocator, name, name.length() == 0 ? column_idx : suffix, candidate))) {
    } else {
      unique = true;
      for (int64_t i = 0; unique && i < column_names_.count(); ++i) {
        unique = !common::ObCharset::case_insensitive_equal(column_names_.at(i), candidate);
      }
      if (unique) {
        ret = column_names_.push_back(candidate);
      } else {
        ++suffix;
      }
    }
  }
  return ret;
}

int ObFileSQLSchema::add_column_names(ObIAllocator &allocator,
                                     const ObIArray<ObString> &fields)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < fields.count(); ++i) {
    if (OB_FAIL(append_unique_column_name(allocator, fields.at(i), i))) {
    }
  }
  return ret;
}

int ObFileSQLSchema::infer_csv_schema(ObFileSQLCSVReader &reader,
                                      ObIAllocator &allocator,
                                      const bool has_header,
                                      ObIArray<ObString> &fields,
                                      const ObString &null_string)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(reader.next_row(fields))) {
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
  } else if (has_header) {
    if (OB_FAIL(add_column_names(allocator, fields))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_names_.count(); ++i) {
        if (OB_FAIL(column_types_.push_back(OB_FILE_SQL_NULL))) {
        } else if (OB_FAIL(column_nullable_.push_back(false))) {
        }
      }
    }
  } else {
    if (OB_FAIL(add_column_names(allocator, fields))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < fields.count(); ++i) {
        const ObFileSQLType value_type = infer_value_type(fields.at(i), null_string);
        if (OB_FAIL(column_types_.push_back(value_type))) {
        } else if (OB_FAIL(column_nullable_.push_back(OB_FILE_SQL_NULL == value_type))) {
        }
      }
    }
  }

  bool is_first_data_row = !has_header && OB_SUCC(ret) && column_names_.count() > 0;
  while (OB_SUCC(ret)) {
    if (is_first_data_row) {
      is_first_data_row = false;
    } else if (OB_FAIL(reader.next_row(fields))) {
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; }
      break;
    }
    if (OB_SUCC(ret)) {
      if (fields.count() > column_types_.count()) {
        ret = OB_INVALID_DATA;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < column_types_.count(); ++i) {
        const ObFileSQLType value_type = i < fields.count()
            ? infer_value_type(fields.at(i), null_string) : OB_FILE_SQL_NULL;
        column_types_.at(i) = merge_types(column_types_.at(i), value_type);
        if (OB_FILE_SQL_NULL == value_type) {
          column_nullable_.at(i) = true;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ret = reader.rewind();
  }
  return ret;
}

int ObFileSQLSchema::infer_jsonl_schema(ObFileSQLCSVReader &reader,
                                        ObIAllocator &allocator,
                                        ObIArray<ObString> &fields)
{
  int ret = OB_SUCCESS;
  reset();
  ObArenaAllocator json_allocator;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(reader.next_row(fields))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      break;
    } else if (fields.count() != 1) {
      ret = OB_INVALID_DATA;
    } else if (fields.at(0).empty()) {
      // JSON Lines permits blank lines between JSON records.
    } else {
      json_allocator.reuse();
      ObJsonNode *root = nullptr;
      if (OB_FAIL(ObJsonParser::get_tree(&json_allocator, fields.at(0), root,
                                         ObJsonParser::JSN_DEFAULT_FLAG))) {
      } else if (OB_ISNULL(root) || ObJsonNodeType::J_OBJECT != root->json_type()) {
        ret = OB_INVALID_DATA;
      } else {
        ObJsonObject *object = static_cast<ObJsonObject *>(root);
        for (uint64_t i = 0; OB_SUCC(ret) && i < object->element_count(); ++i) {
          ObString key;
          ObJsonNode *value = nullptr;
          ObFileSQLType value_type = OB_FILE_SQL_VARCHAR;
          if (OB_FAIL(object->get_value_by_idx(i, key, value))) {
          } else if (OB_ISNULL(value)) {
            ret = OB_INVALID_DATA;
          } else {
            switch (value->json_type()) {
              case ObJsonNodeType::J_NULL:
                value_type = OB_FILE_SQL_NULL;
                break;
              case ObJsonNodeType::J_BOOLEAN:
                value_type = OB_FILE_SQL_BOOLEAN;
                break;
              case ObJsonNodeType::J_INT:
              case ObJsonNodeType::J_UINT:
                value_type = OB_FILE_SQL_BIGINT;
                break;
              case ObJsonNodeType::J_DOUBLE:
              case ObJsonNodeType::J_DECIMAL:
                value_type = OB_FILE_SQL_DOUBLE;
                break;
              case ObJsonNodeType::J_STRING:
                value_type = infer_value_type(static_cast<ObJsonString *>(value)->get_str());
                break;
              case ObJsonNodeType::J_OBJECT:
              case ObJsonNodeType::J_ARRAY:
                ret = OB_NOT_SUPPORTED;
                LOG_USER_ERROR(OB_NOT_SUPPORTED,
                               "nested JSON objects and arrays are not supported in File SQL");
                break;
              default:
                value_type = OB_FILE_SQL_VARCHAR;
                break;
            }
            int64_t column_idx = -1;
            for (int64_t j = 0; j < column_names_.count(); ++j) {
              if (column_names_.at(j) == key) {
                column_idx = j;
                break;
              }
            }
            if (column_idx < 0) {
              ObString owned_key;
              if (OB_FAIL(ob_write_string(allocator, key, owned_key, true))) {
              } else if (OB_FAIL(column_names_.push_back(owned_key))) {
              } else if (OB_FAIL(column_types_.push_back(value_type))) {
              } else if (OB_FAIL(column_nullable_.push_back(true))) {
              }
            } else {
              column_types_.at(column_idx) = merge_types(column_types_.at(column_idx), value_type);
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ret = reader.rewind();
  }
  return ret;
}

namespace
{
// Minimal Thrift Compact Protocol reader for Parquet's flat-file metadata.
// Keeping this parser local avoids introducing a server-side Arrow/Parquet
// dependency for FileSQL.
struct FileSQLCompactReader
{
  enum CompactType : uint8_t {
    CT_STOP = 0, CT_TRUE = 1, CT_FALSE = 2, CT_BYTE = 3, CT_I16 = 4,
    CT_I32 = 5, CT_I64 = 6, CT_DOUBLE = 7, CT_BINARY = 8, CT_LIST = 9,
    CT_SET = 10, CT_MAP = 11, CT_STRUCT = 12
  };
  FileSQLCompactReader(const char *data, const int64_t length, const int64_t offset = 0)
      : data_(data), length_(length), pos_(offset) {}
  int read_varint(uint64_t &value)
  {
    int ret = OB_SUCCESS;
    value = 0;
    int shift = 0;
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      if (pos_ >= length_ || shift >= 64) {
        ret = OB_INVALID_DATA;
      } else {
        const uint8_t byte = static_cast<uint8_t>(data_[pos_++]);
        value |= static_cast<uint64_t>(byte & 0x7f) << shift;
        done = 0 == (byte & 0x80);
        shift += 7;
      }
    }
    return ret;
  }
  int read_signed(int64_t &value)
  {
    uint64_t encoded = 0;
    int ret = read_varint(encoded);
    if (OB_SUCC(ret)) {
      value = static_cast<int64_t>((encoded >> 1) ^ (0ULL - (encoded & 1)));
    }
    return ret;
  }
  int read_field(const int32_t previous_id, int32_t &field_id, uint8_t &type, bool &is_stop)
  {
    int ret = OB_SUCCESS;
    is_stop = false;
    if (pos_ >= length_) {
      ret = OB_INVALID_DATA;
    } else {
      const uint8_t header = static_cast<uint8_t>(data_[pos_++]);
      type = header & 0x0f;
      if (CT_STOP == type) {
        is_stop = true;
      } else {
        const int32_t delta = header >> 4;
        if (0 != delta) {
          field_id = previous_id + delta;
        } else {
          int64_t signed_id = 0;
          if (OB_FAIL(read_signed(signed_id))) {
          } else if (signed_id < INT32_MIN || signed_id > INT32_MAX) {
            ret = OB_INVALID_DATA;
          } else {
            field_id = static_cast<int32_t>(signed_id);
          }
        }
      }
    }
    return ret;
  }
  int read_list(uint64_t &count, uint8_t &element_type)
  {
    int ret = OB_SUCCESS;
    if (pos_ >= length_) {
      ret = OB_INVALID_DATA;
    } else {
      const uint8_t header = static_cast<uint8_t>(data_[pos_++]);
      count = header >> 4;
      element_type = header & 0x0f;
      if (15 == count) {
        ret = read_varint(count);
      }
    }
    return ret;
  }
  int read_i64(int64_t &value) { return read_signed(value); }
  int read_binary(ObString &value)
  {
    int ret = OB_SUCCESS;
    uint64_t size = 0;
    if (OB_FAIL(read_varint(size))) {
    } else if (size > static_cast<uint64_t>(length_ - pos_)) {
      ret = OB_INVALID_DATA;
    } else if (size > INT32_MAX) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      value.assign_ptr(const_cast<char *>(data_ + pos_), static_cast<int32_t>(size));
      pos_ += static_cast<int64_t>(size);
    }
    return ret;
  }
  int skip(const uint8_t type)
  {
    int ret = OB_SUCCESS;
    uint64_t size = 0;
    uint8_t element_type = CT_STOP;
    switch (type) {
      case CT_TRUE: case CT_FALSE: break; // bool value is encoded in the field header
      case CT_BYTE:
        if (pos_ >= length_) ret = OB_INVALID_DATA; else ++pos_;
        break;
      case CT_I16: case CT_I32: case CT_I64:
        ret = read_varint(size);
        break;
      case CT_DOUBLE:
        if (length_ - pos_ < 8) ret = OB_INVALID_DATA; else pos_ += 8;
        break;
      case CT_BINARY:
        if (OB_FAIL(read_varint(size))) {
        } else if (size > static_cast<uint64_t>(length_ - pos_)) ret = OB_INVALID_DATA;
        else pos_ += static_cast<int64_t>(size);
        break;
      case CT_LIST: case CT_SET:
        if (OB_FAIL(read_list(size, element_type))) {
        } else {
          for (uint64_t i = 0; OB_SUCC(ret) && i < size; ++i) ret = skip(element_type);
        }
        break;
      case CT_MAP:
        if (OB_FAIL(read_varint(size))) {
        } else if (0 != size) {
          if (pos_ >= length_) ret = OB_INVALID_DATA;
          else {
            const uint8_t types = static_cast<uint8_t>(data_[pos_++]);
            const uint8_t key_type = types >> 4;
            const uint8_t value_type = types & 0x0f;
            for (uint64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
              if (OB_FAIL(skip(key_type))) {
              } else if (OB_FAIL(skip(value_type))) {
              }
            }
          }
        }
        break;
      case CT_STRUCT: {
        int32_t previous_id = 0;
        bool done = false;
        while (OB_SUCC(ret) && !done) {
          int32_t field_id = 0;
          uint8_t field_type = CT_STOP;
          if (OB_FAIL(read_field(previous_id, field_id, field_type, done))) {
          } else if (!done) {
            previous_id = field_id;
            ret = skip(field_type);
          }
        }
        break;
      }
      default: ret = OB_INVALID_DATA; break;
    }
    return ret;
  }
  const char *data_;
  int64_t length_;
  int64_t pos_;
};

int file_sql_read_i32(FileSQLCompactReader &reader, int32_t &value)
{
  int64_t tmp = 0;
  int ret = reader.read_signed(tmp);
  if (OB_SUCC(ret) && (tmp < INT32_MIN || tmp > INT32_MAX)) ret = OB_INVALID_DATA;
  if (OB_SUCC(ret)) value = static_cast<int32_t>(tmp);
  return ret;
}

int file_sql_parse_schema_element(FileSQLCompactReader &reader,
                                  ObIAllocator &allocator,
                                  FileSQLParquetSchemaColumn &column)
{
  int ret = OB_SUCCESS;
  int32_t previous_id = 0;
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    int32_t id = 0;
    uint8_t type = FileSQLCompactReader::CT_STOP;
    if (OB_FAIL(reader.read_field(previous_id, id, type, done))) {
    } else if (!done) {
      previous_id = id;
      if (1 == id) {
        if (OB_FAIL(file_sql_read_i32(reader, column.physical_type_))) {
        }
      } else if (3 == id) {
        if (OB_FAIL(file_sql_read_i32(reader, column.repetition_type_))) {
        }
      } else if (4 == id) {
        ObString name;
        ObString owned_name;
        if (OB_FAIL(reader.read_binary(name))) {
        } else if (OB_FAIL(ob_write_string(allocator, name, owned_name, true))) {
        } else {
          column.name_ = owned_name;
        }
      } else if (5 == id) {
        if (OB_FAIL(file_sql_read_i32(reader, column.child_count_))) {
        }
      } else if (OB_FAIL(reader.skip(type))) {
      }
    }
  }
  return ret;
}

int file_sql_parse_column_meta(FileSQLCompactReader &reader, FileSQLParquetColumnMeta &meta)
{
  int ret = OB_SUCCESS;
  int32_t previous_id = 0;
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    int32_t id = 0;
    uint8_t type = FileSQLCompactReader::CT_STOP;
    if (OB_FAIL(reader.read_field(previous_id, id, type, done))) {
    } else if (!done) {
      previous_id = id;
      if (1 == id) {
        if (OB_FAIL(file_sql_read_i32(reader, meta.physical_type_))) {
        }
      } else if (4 == id) {
        if (OB_FAIL(file_sql_read_i32(reader, meta.codec_))) {
        }
      } else if (5 == id) {
        if (OB_FAIL(reader.read_i64(meta.value_count_))) {
        }
      } else if (6 == id) {
        if (OB_FAIL(reader.read_i64(meta.total_uncompressed_size_))) {
        }
      } else if (7 == id) {
        if (OB_FAIL(reader.read_i64(meta.compressed_size_))) {
        }
      } else if (9 == id) {
        if (OB_FAIL(reader.read_i64(meta.data_page_offset_))) {
        }
      } else if (11 == id) {
        if (OB_FAIL(reader.read_i64(meta.dictionary_page_offset_))) {
        }
      } else if (OB_FAIL(reader.skip(type))) {
      }
    }
  }
  return ret;
}

int file_sql_parse_column_chunk(FileSQLCompactReader &reader, FileSQLParquetColumnMeta &meta)
{
  int ret = OB_SUCCESS;
  int32_t previous_id = 0;
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    int32_t id = 0;
    uint8_t type = FileSQLCompactReader::CT_STOP;
    if (OB_FAIL(reader.read_field(previous_id, id, type, done))) {
    } else if (!done) {
      previous_id = id;
      if (3 == id && FileSQLCompactReader::CT_STRUCT == type) {
        if (OB_FAIL(file_sql_parse_column_meta(reader, meta))) {
        }
      } else if (OB_FAIL(reader.skip(type))) {
      }
    }
  }
  return ret;
}

int file_sql_parse_row_group(FileSQLCompactReader &reader,
                             ObIAllocator &allocator,
                             ObIArray<FileSQLParquetColumnMeta> &columns,
                             int64_t &row_count)
{
  int ret = OB_SUCCESS;
  int32_t previous_id = 0;
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    int32_t id = 0;
    uint8_t type = FileSQLCompactReader::CT_STOP;
    if (OB_FAIL(reader.read_field(previous_id, id, type, done))) {
    } else if (!done) {
      previous_id = id;
      if (3 == id) {
        if (OB_FAIL(reader.read_i64(row_count))) {
        }
      } else if (1 == id && FileSQLCompactReader::CT_LIST == type) {
        uint64_t count = 0;
        uint8_t element_type = FileSQLCompactReader::CT_STOP;
        if (OB_FAIL(reader.read_list(count, element_type))) {
        } else if (FileSQLCompactReader::CT_STRUCT != element_type || count > 1024) {
          ret = OB_NOT_SUPPORTED;
        } else {
          for (uint64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
            FileSQLParquetColumnMeta meta;
            if (OB_FAIL(file_sql_parse_column_chunk(reader, meta))) {
            } else if (OB_FAIL(columns.push_back(meta))) {
            }
          }
        }
      } else if (OB_FAIL(reader.skip(type))) {
      }
    }
  }
  UNUSED(allocator);
  return ret;
}

int file_sql_parse_file_metadata(FileSQLCompactReader &reader,
                                 ObIAllocator &allocator,
                                 ObIArray<FileSQLParquetSchemaColumn> &schema,
                                 ObIArray<FileSQLParquetColumnMeta> &columns,
                                 ObIArray<int64_t> &row_group_counts,
                                 int64_t &row_count)
{
  int ret = OB_SUCCESS;
  int32_t previous_id = 0;
  bool done = false;
  int64_t footer_row_count = -1;
  row_count = 0;
  while (OB_SUCC(ret) && !done) {
    int32_t id = 0;
    uint8_t type = FileSQLCompactReader::CT_STOP;
    if (OB_FAIL(reader.read_field(previous_id, id, type, done))) {
    } else if (!done) {
      previous_id = id;
      if (2 == id && FileSQLCompactReader::CT_LIST == type) {
        uint64_t count = 0;
        uint8_t element_type = FileSQLCompactReader::CT_STOP;
        if (OB_FAIL(reader.read_list(count, element_type))) {
        } else if (FileSQLCompactReader::CT_STRUCT != element_type || count > 1025) {
          ret = OB_NOT_SUPPORTED;
        } else {
          for (uint64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
            FileSQLParquetSchemaColumn column;
            if (OB_FAIL(file_sql_parse_schema_element(reader, allocator, column))) {
            } else if (OB_FAIL(schema.push_back(column))) {
            }
          }
        }
      } else if (3 == id) {
        if (OB_FAIL(reader.read_i64(footer_row_count))) {
        }
      } else if (4 == id && FileSQLCompactReader::CT_LIST == type) {
        uint64_t count = 0;
        uint8_t element_type = FileSQLCompactReader::CT_STOP;
        if (OB_FAIL(reader.read_list(count, element_type))) {
        } else if (FileSQLCompactReader::CT_STRUCT != element_type || 0 == count || count > 1024) {
          ret = OB_NOT_SUPPORTED;
        } else {
          for (uint64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
            int64_t group_row_count = 0;
            if (OB_FAIL(file_sql_parse_row_group(reader, allocator, columns, group_row_count))) {
            } else if (group_row_count <= 0 || group_row_count > INT32_MAX) {
              ret = OB_NOT_SUPPORTED;
            } else if (OB_FAIL(row_group_counts.push_back(group_row_count))) {
            } else {
              row_count += group_row_count;
            }
          }
        }
      } else if (OB_FAIL(reader.skip(type))) {
      }
    }
  }
  if (OB_SUCC(ret) && (footer_row_count != row_count || schema.count() < 2
                       || row_group_counts.count() <= 0
                       || columns.count() != row_group_counts.count() * (schema.count() - 1))) {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int file_sql_parse_data_page(FileSQLCompactReader &reader,
                             int32_t &page_type,
                             int64_t &uncompressed_size,
                             int64_t &compressed_size,
                             int64_t &value_count,
                             int64_t &dictionary_value_count,
                             int32_t &encoding,
                             int32_t &dictionary_encoding,
                             int32_t &definition_encoding,
                             int32_t &repetition_encoding,
                             int64_t &definition_length,
                             int64_t &repetition_length,
                             bool &values_compressed)
{
  int ret = OB_SUCCESS;
  int32_t previous_id = 0;
  bool done = false;
  page_type = -1;
  uncompressed_size = compressed_size = value_count = -1;
  dictionary_value_count = -1;
  definition_length = repetition_length = 0;
  encoding = dictionary_encoding = definition_encoding = repetition_encoding = -1;
  values_compressed = true;
  while (OB_SUCC(ret) && !done) {
    int32_t id = 0;
    uint8_t type = FileSQLCompactReader::CT_STOP;
    if (OB_FAIL(reader.read_field(previous_id, id, type, done))) {
    } else if (!done) {
      previous_id = id;
      if (1 == id) {
        if (OB_FAIL(file_sql_read_i32(reader, page_type))) {
        }
      } else if (2 == id) {
        if (OB_FAIL(reader.read_i64(uncompressed_size))) {
        }
      } else if (3 == id) {
        if (OB_FAIL(reader.read_i64(compressed_size))) {
        }
      } else if (5 == id && FileSQLCompactReader::CT_STRUCT == type) {
        // DataPageHeader (V1).
        int32_t data_previous_id = 0;
        bool data_done = false;
        while (OB_SUCC(ret) && !data_done) {
          int32_t data_id = 0;
          uint8_t data_type = FileSQLCompactReader::CT_STOP;
          if (OB_FAIL(reader.read_field(data_previous_id, data_id, data_type, data_done))) {
          } else if (!data_done) {
            data_previous_id = data_id;
            if (1 == data_id) {
              if (OB_FAIL(reader.read_i64(value_count))) {
              }
            } else if (2 == data_id) {
              if (OB_FAIL(file_sql_read_i32(reader, encoding))) {
              }
            } else if (3 == data_id) {
              if (OB_FAIL(file_sql_read_i32(reader, definition_encoding))) {
              }
            } else if (4 == data_id) {
              if (OB_FAIL(file_sql_read_i32(reader, repetition_encoding))) {
              }
            } else if (OB_FAIL(reader.skip(data_type))) {
            }
          }
        }
      } else if (7 == id && FileSQLCompactReader::CT_STRUCT == type) {
        // DictionaryPageHeader.
        int32_t dict_previous_id = 0;
        bool dict_done = false;
        while (OB_SUCC(ret) && !dict_done) {
          int32_t dict_id = 0;
          uint8_t dict_type = FileSQLCompactReader::CT_STOP;
          if (OB_FAIL(reader.read_field(dict_previous_id, dict_id, dict_type, dict_done))) {
          } else if (!dict_done) {
            dict_previous_id = dict_id;
            if (1 == dict_id) {
              if (OB_FAIL(reader.read_i64(dictionary_value_count))) {
              }
            } else if (2 == dict_id) {
              if (OB_FAIL(file_sql_read_i32(reader, dictionary_encoding))) {
              }
            } else if (OB_FAIL(reader.skip(dict_type))) {
            }
          }
        }
      } else if (8 == id && FileSQLCompactReader::CT_STRUCT == type) {
        int32_t data_previous_id = 0;
        bool data_done = false;
        while (OB_SUCC(ret) && !data_done) {
          int32_t data_id = 0;
          uint8_t data_type = FileSQLCompactReader::CT_STOP;
          if (OB_FAIL(reader.read_field(data_previous_id, data_id, data_type, data_done))) {
          } else if (!data_done) {
            data_previous_id = data_id;
            if (1 == data_id) {
              if (OB_FAIL(reader.read_i64(value_count))) {
              }
            } else if (4 == data_id) {
              if (OB_FAIL(file_sql_read_i32(reader, encoding))) {
              }
            } else if (5 == data_id) {
              if (OB_FAIL(reader.read_i64(definition_length))) {
              }
            } else if (6 == data_id) {
              if (OB_FAIL(reader.read_i64(repetition_length))) {
              }
            } else if (7 == data_id) {
              if (FileSQLCompactReader::CT_TRUE == data_type) values_compressed = true;
              else if (FileSQLCompactReader::CT_FALSE == data_type) values_compressed = false;
              else ret = OB_INVALID_DATA;
            } else if (OB_FAIL(reader.skip(data_type))) {
            }
          }
        }
      } else if (OB_FAIL(reader.skip(type))) {
      }
    }
  }
  return ret;
}

int file_sql_snappy_decompress(const char *src, const int64_t src_len,
                               char *dst, const int64_t dst_len)
{
  int ret = OB_SUCCESS;
  uint64_t expected = 0;
  int64_t src_pos = 0;
  int64_t dst_pos = 0;
  int shift = 0;
  bool length_done = false;
  while (OB_SUCC(ret) && !length_done) {
    if (src_pos >= src_len || shift >= 64) ret = OB_INVALID_DATA;
    else {
      const uint8_t byte = static_cast<uint8_t>(src[src_pos++]);
      expected |= static_cast<uint64_t>(byte & 0x7f) << shift;
      length_done = 0 == (byte & 0x80);
      shift += 7;
    }
  }
  if (OB_SUCC(ret) && expected != static_cast<uint64_t>(dst_len)) ret = OB_INVALID_DATA;
  while (OB_SUCC(ret) && src_pos < src_len && dst_pos < dst_len) {
    const uint8_t tag = static_cast<uint8_t>(src[src_pos++]);
    const uint8_t kind = tag & 3;
    int64_t length = 0;
    int64_t offset = 0;
    if (0 == kind) {
      const uint8_t code = tag >> 2;
      if (code < 60) {
        length = code + 1;
      } else {
        const int byte_count = code - 59;
        uint64_t encoded_len = 0;
        if (byte_count > 4 || src_len - src_pos < byte_count) ret = OB_INVALID_DATA;
        for (int i = 0; OB_SUCC(ret) && i < byte_count; ++i) {
          encoded_len |= static_cast<uint64_t>(static_cast<uint8_t>(src[src_pos++])) << (8 * i);
        }
        length = static_cast<int64_t>(encoded_len + 1);
      }
      if (OB_SUCC(ret) && (length > src_len - src_pos || length > dst_len - dst_pos)) ret = OB_INVALID_DATA;
      if (OB_SUCC(ret)) {
        MEMCPY(dst + dst_pos, src + src_pos, length);
        src_pos += length;
        dst_pos += length;
      }
    } else {
      if (1 == kind) {
        length = 4 + ((tag >> 2) & 7);
        if (src_pos >= src_len) ret = OB_INVALID_DATA;
        else offset = ((tag & 0xe0) << 3) | static_cast<uint8_t>(src[src_pos++]);
      } else if (2 == kind) {
        length = 1 + (tag >> 2);
        if (src_len - src_pos < 2) ret = OB_INVALID_DATA;
        else {
          offset = static_cast<uint8_t>(src[src_pos])
                 | (static_cast<int64_t>(static_cast<uint8_t>(src[src_pos + 1])) << 8);
          src_pos += 2;
        }
      } else {
        length = 1 + (tag >> 2);
        if (src_len - src_pos < 4) ret = OB_INVALID_DATA;
        else {
          offset = static_cast<uint8_t>(src[src_pos])
                 | (static_cast<int64_t>(static_cast<uint8_t>(src[src_pos + 1])) << 8)
                 | (static_cast<int64_t>(static_cast<uint8_t>(src[src_pos + 2])) << 16)
                 | (static_cast<int64_t>(static_cast<uint8_t>(src[src_pos + 3])) << 24);
          src_pos += 4;
        }
      }
      if (OB_SUCC(ret) && (offset <= 0 || offset > dst_pos || length > dst_len - dst_pos)) ret = OB_INVALID_DATA;
      for (int64_t i = 0; OB_SUCC(ret) && i < length; ++i) {
        dst[dst_pos] = dst[dst_pos - offset];
        ++dst_pos;
      }
    }
  }
  if (OB_SUCC(ret) && (dst_pos != dst_len || src_pos != src_len)) ret = OB_INVALID_DATA;
  return ret;
}

int file_sql_decode_definition_levels(const char *data, const int64_t length,
                                      const int64_t value_count,
                                      ObIAllocator &allocator, int8_t *&levels)
{
  int ret = OB_SUCCESS;
  levels = nullptr;
  if (OB_ISNULL(levels = static_cast<int8_t *>(allocator.alloc(value_count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    int64_t pos = 0;
    int64_t output = 0;
    while (OB_SUCC(ret) && output < value_count) {
      uint64_t header = 0;
      int shift = 0;
      bool done = false;
      while (OB_SUCC(ret) && !done) {
        if (pos >= length || shift >= 64) ret = OB_INVALID_DATA;
        else {
          const uint8_t byte = static_cast<uint8_t>(data[pos++]);
          header |= static_cast<uint64_t>(byte & 0x7f) << shift;
          done = 0 == (byte & 0x80);
          shift += 7;
        }
      }
      if (OB_SUCC(ret) && 0 != (header & 1)) {
        const uint64_t groups = header >> 1;
        const uint64_t packed_bytes = groups; // max definition level is one bit
        if (groups == 0 || packed_bytes > static_cast<uint64_t>(length - pos)) ret = OB_INVALID_DATA;
        for (uint64_t group = 0; OB_SUCC(ret) && group < groups; ++group) {
          const uint8_t packed = static_cast<uint8_t>(data[pos++]);
          for (int bit = 0; bit < 8 && output < value_count; ++bit) {
            levels[output++] = static_cast<int8_t>((packed >> bit) & 1);
          }
        }
      } else if (OB_SUCC(ret)) {
        const uint64_t run_length = header >> 1;
        if (0 == run_length || pos >= length) ret = OB_INVALID_DATA;
        else {
          const uint8_t level = static_cast<uint8_t>(data[pos++]);
          if (level > 1) ret = OB_INVALID_DATA;
          for (uint64_t i = 0; OB_SUCC(ret) && i < run_length && output < value_count; ++i) {
            levels[output++] = static_cast<int8_t>(level);
          }
        }
      }
    }
    if (OB_SUCC(ret) && pos != length) ret = OB_INVALID_DATA;
  }
  return ret;
}

int file_sql_decode_dictionary_indices(const char *data, const int64_t length,
                                       const int64_t value_count,
                                       const int64_t dictionary_size,
                                       ObIArray<int64_t> &indices)
{
  int ret = OB_SUCCESS;
  indices.reset();
  if (value_count < 0 || dictionary_size <= 0 || length < 0) {
    ret = OB_INVALID_DATA;
  } else if (0 == value_count) {
    if (length > 1 || (1 == length
                       && (static_cast<uint8_t>(data[0]) > 32
                           || (static_cast<uint8_t>(data[0]) < 32
                               && (static_cast<uint64_t>(1)
                                   << static_cast<uint8_t>(data[0])) < dictionary_size)))) {
      ret = OB_INVALID_DATA;
    }
  } else if (length < 1) {
    ret = OB_INVALID_DATA;
  } else {
    int64_t pos = 0;
    const uint8_t bit_width = static_cast<uint8_t>(data[pos++]);
    if (bit_width > 32
        || (bit_width < 32 && (static_cast<uint64_t>(1) << bit_width) < dictionary_size)) {
      ret = OB_INVALID_DATA;
    }
    int64_t decoded_count = 0;
    while (OB_SUCC(ret) && decoded_count < value_count) {
      uint64_t header = 0;
      int shift = 0;
      bool done = false;
      while (OB_SUCC(ret) && !done) {
        if (pos >= length || shift >= 64) {
          ret = OB_INVALID_DATA;
        } else {
          const uint8_t byte = static_cast<uint8_t>(data[pos++]);
          header |= static_cast<uint64_t>(byte & 0x7f) << shift;
          done = 0 == (byte & 0x80);
          shift += 7;
        }
      }
      if (OB_SUCC(ret) && 0 == (header & 1)) {
        const uint64_t run_length = header >> 1;
        const int64_t value_byte_width = (bit_width + 7) / 8;
        if (0 == run_length || run_length > static_cast<uint64_t>(value_count - decoded_count)
            || value_byte_width > length - pos) {
          ret = OB_INVALID_DATA;
        } else {
          uint64_t value = 0;
          for (int64_t i = 0; i < value_byte_width; ++i) {
            value |= static_cast<uint64_t>(static_cast<uint8_t>(data[pos++])) << (8 * i);
          }
          if (value >= static_cast<uint64_t>(dictionary_size)) {
            ret = OB_INVALID_DATA;
          }
          for (uint64_t i = 0; OB_SUCC(ret) && i < run_length; ++i) {
            if (OB_FAIL(indices.push_back(static_cast<int64_t>(value)))) {
            }
          }
          if (OB_SUCC(ret)) decoded_count += static_cast<int64_t>(run_length);
        }
      } else if (OB_SUCC(ret)) {
        const uint64_t group_count = header >> 1;
        const uint64_t max_groups = static_cast<uint64_t>(value_count - decoded_count + 7) / 8;
        if (0 == group_count || group_count > max_groups
            || group_count > static_cast<uint64_t>(INT64_MAX / 8)
            || group_count * bit_width > static_cast<uint64_t>(length - pos)) {
          ret = OB_INVALID_DATA;
        } else {
          const uint64_t packed_count = group_count * 8;
          for (uint64_t i = 0; OB_SUCC(ret) && i < packed_count
               && decoded_count < value_count; ++i) {
            uint64_t value = 0;
            const uint64_t bit_offset = i * bit_width;
            for (uint8_t bit = 0; bit < bit_width; ++bit) {
              const uint64_t current_bit = bit_offset + bit;
              const uint8_t packed = static_cast<uint8_t>(data[pos + current_bit / 8]);
              value |= static_cast<uint64_t>((packed >> (current_bit % 8)) & 1) << bit;
            }
            if (value >= static_cast<uint64_t>(dictionary_size)) {
              ret = OB_INVALID_DATA;
            } else if (OB_FAIL(indices.push_back(static_cast<int64_t>(value)))) {
            } else {
              ++decoded_count;
            }
          }
          if (OB_SUCC(ret)) pos += static_cast<int64_t>(group_count * bit_width);
        }
      }
    }
    if (OB_SUCC(ret) && (decoded_count != value_count || indices.count() != value_count
                         || pos != length)) {
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

int file_sql_decode_dictionary_values(const char *data, const int64_t length,
                                      const int64_t value_count, const int8_t *levels,
                                      const ObIArray<ObString> &dictionary_values,
                                      ObIArray<ObString> &column_values)
{
  int ret = OB_SUCCESS;
  int64_t non_null_count = 0;
  for (int64_t i = 0; i < value_count; ++i) {
    if (0 != levels[i]) ++non_null_count;
  }
  ObSEArray<int64_t, 16> indices;
  if (OB_FAIL(file_sql_decode_dictionary_indices(data, length, non_null_count,
                                                dictionary_values.count(), indices))) {
  } else {
    int64_t index_pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < value_count; ++i) {
      if (0 == levels[i]) {
        ObString null_value;
        if (OB_FAIL(column_values.push_back(null_value))) {
        }
      } else if (index_pos >= indices.count()) {
        ret = OB_INVALID_DATA;
      } else {
        const int64_t dictionary_index = indices.at(index_pos++);
        if (dictionary_index < 0 || dictionary_index >= dictionary_values.count()) {
          ret = OB_INVALID_DATA;
        } else if (OB_FAIL(column_values.push_back(dictionary_values.at(dictionary_index)))) {
        }
      }
    }
    if (OB_SUCC(ret) && index_pos != indices.count()) ret = OB_INVALID_DATA;
  }
  return ret;
}

int file_sql_append_parquet_value(ObIAllocator &allocator, const char *data,
                                  const int64_t length, ObString &value)
{
  int ret = OB_SUCCESS;
  char *owned = nullptr;
  if (length < 0 || length > INT32_MAX) {
    ret = OB_INVALID_DATA;
  } else if (0 == length) {
    value.reset();
  } else if (OB_ISNULL(owned = static_cast<char *>(allocator.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(owned, data, length);
    value.assign_ptr(owned, static_cast<int32_t>(length));
  }
  return ret;
}

int file_sql_decode_plain_values(const char *data, const int64_t length,
                                 const int32_t physical_type, const int64_t count,
                                 const int8_t *levels, ObIAllocator &allocator,
                                 ObIArray<ObString> &values)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObString value;
    if (0 != levels[i]) {
      char text[128];
      int n = 0;
      if (1 == physical_type) {
        if (length - pos < 4) ret = OB_INVALID_DATA;
        else {
          const uint32_t raw = static_cast<uint32_t>(static_cast<uint8_t>(data[pos]))
                             | (static_cast<uint32_t>(static_cast<uint8_t>(data[pos + 1])) << 8)
                             | (static_cast<uint32_t>(static_cast<uint8_t>(data[pos + 2])) << 16)
                             | (static_cast<uint32_t>(static_cast<uint8_t>(data[pos + 3])) << 24);
          pos += 4;
          n = snprintf(text, sizeof(text), "%d", static_cast<int32_t>(raw));
        }
      } else if (2 == physical_type) {
        if (length - pos < 8) ret = OB_INVALID_DATA;
        else {
          uint64_t raw = 0;
          for (int j = 0; j < 8; ++j) raw |= static_cast<uint64_t>(static_cast<uint8_t>(data[pos + j])) << (8 * j);
          pos += 8;
          n = snprintf(text, sizeof(text), "%lld", static_cast<long long>(static_cast<int64_t>(raw)));
        }
      } else if (4 == physical_type) {
        if (length - pos < 4) ret = OB_INVALID_DATA;
        else {
          uint32_t raw = static_cast<uint32_t>(static_cast<uint8_t>(data[pos]))
                       | (static_cast<uint32_t>(static_cast<uint8_t>(data[pos + 1])) << 8)
                       | (static_cast<uint32_t>(static_cast<uint8_t>(data[pos + 2])) << 16)
                       | (static_cast<uint32_t>(static_cast<uint8_t>(data[pos + 3])) << 24);
          float number = 0;
          MEMCPY(&number, &raw, sizeof(number));
          pos += 4;
          n = snprintf(text, sizeof(text), "%.9g", static_cast<double>(number));
        }
      } else if (5 == physical_type) {
        if (length - pos < 8) ret = OB_INVALID_DATA;
        else {
          uint64_t raw = 0;
          for (int j = 0; j < 8; ++j) raw |= static_cast<uint64_t>(static_cast<uint8_t>(data[pos + j])) << (8 * j);
          double number = 0;
          MEMCPY(&number, &raw, sizeof(number));
          pos += 8;
          n = snprintf(text, sizeof(text), "%.17g", number);
        }
      } else if (6 == physical_type) {
        if (length - pos < 4) ret = OB_INVALID_DATA;
        else {
          int32_t str_len = static_cast<int32_t>(static_cast<uint8_t>(data[pos]))
                          | (static_cast<int32_t>(static_cast<uint8_t>(data[pos + 1])) << 8)
                          | (static_cast<int32_t>(static_cast<uint8_t>(data[pos + 2])) << 16)
                          | (static_cast<int32_t>(static_cast<uint8_t>(data[pos + 3])) << 24);
          pos += 4;
          if (str_len < 0 || str_len > length - pos) ret = OB_INVALID_DATA;
          else if (OB_FAIL(file_sql_append_parquet_value(allocator, data + pos, str_len, value))) {
          } else pos += str_len;
        }
      } else if (0 == physical_type) {
        if (pos >= length) ret = OB_INVALID_DATA;
        else {
          const int bit = static_cast<uint8_t>(data[pos]) >> (i & 7) & 1;
          if (7 == (i & 7)) ++pos;
          n = snprintf(text, sizeof(text), "%d", bit);
        }
      } else {
        ret = OB_NOT_SUPPORTED;
      }
      if (OB_SUCC(ret) && physical_type != 6) {
        if (n < 0 || n >= static_cast<int>(sizeof(text))) ret = OB_INVALID_DATA;
        else if (OB_FAIL(file_sql_append_parquet_value(allocator, text, n, value))) {
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(values.push_back(value))) {
    }
  }
  if (OB_SUCC(ret) && pos != length) ret = OB_INVALID_DATA;
  return ret;
}

int file_sql_decode_parquet_column_chunk(const char *chunk_data,
                                         const int64_t chunk_size,
                                         const FileSQLParquetSchemaColumn &schema_column,
                                         const FileSQLParquetColumnMeta &column_meta,
                                         const int64_t row_count,
                                         ObIAllocator &scratch_allocator,
                                         ObIAllocator &value_allocator,
                                         ObIArray<ObString> &column_values)
{
  int ret = OB_SUCCESS;
  if (row_count <= 0 || schema_column.child_count_ != 0 || schema_column.name_.empty()
      || column_meta.data_page_offset_ < 0 || column_meta.data_page_offset_ >= chunk_size
      || column_meta.value_count_ != row_count
      || (column_meta.dictionary_page_offset_ >= 0
          && column_meta.dictionary_page_offset_ >= column_meta.data_page_offset_)
      || column_meta.compressed_size_ <= 0
      || column_meta.compressed_size_ != chunk_size
      || column_meta.total_uncompressed_size_ <= 0
      || column_meta.total_uncompressed_size_ > 64LL * 1024 * 1024
      || (column_meta.codec_ != 0 && column_meta.codec_ != 1)
      || column_meta.physical_type_ != schema_column.physical_type_) {
    ret = OB_NOT_SUPPORTED;
  } else {
    const int64_t chunk_start = 0;
    const int64_t chunk_end = chunk_size;
    int64_t page_offset = chunk_start;
    int64_t decoded_value_count = 0;
    int64_t decoded_uncompressed_size = 0;
    ObSEArray<ObString, 16> dictionary_values;
    if (column_meta.dictionary_page_offset_ >= 0) {
      FileSQLCompactReader dictionary_header(chunk_data, chunk_end, page_offset);
      int32_t page_type = -1;
      int64_t uncompressed_size = 0;
      int64_t compressed_size = 0;
      int64_t value_count = -1;
      int64_t dictionary_value_count = -1;
      int32_t encoding = -1;
      int32_t dictionary_encoding = -1;
      int32_t definition_encoding = -1;
      int32_t repetition_encoding = -1;
      int64_t definition_length = 0;
      int64_t repetition_length = 0;
      bool values_compressed = true;
      if (OB_FAIL(file_sql_parse_data_page(dictionary_header, page_type, uncompressed_size,
                                           compressed_size, value_count, dictionary_value_count,
                                           encoding, dictionary_encoding, definition_encoding,
                                           repetition_encoding, definition_length,
                                           repetition_length, values_compressed))) {
      } else if (page_type != 2 || dictionary_value_count <= 0 || dictionary_encoding != 0
                 || compressed_size < 0 || uncompressed_size < 0
                 || dictionary_header.pos_ > chunk_end
                 || compressed_size > chunk_end - dictionary_header.pos_
                 || uncompressed_size > 64LL * 1024 * 1024
                 || uncompressed_size > column_meta.total_uncompressed_size_ - decoded_uncompressed_size) {
        ret = OB_NOT_SUPPORTED;
      } else {
        decoded_uncompressed_size += uncompressed_size;
        char *plain_dictionary = nullptr;
        const int64_t dictionary_buffer_size = OB_MAX(1, uncompressed_size);
        const char *body = chunk_data + dictionary_header.pos_;
        if (OB_ISNULL(plain_dictionary = static_cast<char *>(scratch_allocator.alloc(dictionary_buffer_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (column_meta.codec_ == 1) {
          ret = file_sql_snappy_decompress(body, compressed_size,
                                           plain_dictionary, uncompressed_size);
        } else if (compressed_size != uncompressed_size) {
          ret = OB_INVALID_DATA;
        } else {
          MEMCPY(plain_dictionary, body, uncompressed_size);
        }
        int8_t *dictionary_levels = nullptr;
        if (OB_SUCC(ret) && OB_ISNULL(dictionary_levels = static_cast<int8_t *>(
                                                scratch_allocator.alloc(dictionary_value_count)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_SUCC(ret)) {
          MEMSET(dictionary_levels, 1, dictionary_value_count);
        }
        if (OB_SUCC(ret) && OB_FAIL(file_sql_decode_plain_values(
                plain_dictionary, uncompressed_size, schema_column.physical_type_,
                dictionary_value_count, dictionary_levels, value_allocator, dictionary_values))) {
        }
        if (OB_SUCC(ret)) {
          page_offset = dictionary_header.pos_ + compressed_size;
          if (page_offset != column_meta.data_page_offset_) ret = OB_INVALID_DATA;
        }
      }
    }
    while (OB_SUCC(ret) && page_offset < chunk_end && decoded_value_count < row_count) {
      FileSQLCompactReader page_header(chunk_data, chunk_end, page_offset);
      int32_t page_type = -1;
      int64_t uncompressed_size = 0;
      int64_t compressed_size = 0;
      int64_t page_value_count = 0;
      int64_t dictionary_value_count = -1;
      int32_t encoding = -1;
      int32_t dictionary_encoding = -1;
      int32_t definition_encoding = -1;
      int32_t repetition_encoding = -1;
      int64_t definition_length = 0;
      int64_t repetition_length = 0;
      bool values_compressed = true;
      if (OB_FAIL(file_sql_parse_data_page(page_header, page_type, uncompressed_size,
                                           compressed_size, page_value_count,
                                           dictionary_value_count, encoding, dictionary_encoding,
                                           definition_encoding, repetition_encoding,
                                           definition_length, repetition_length,
                                           values_compressed))) {
      } else if ((page_type != 0 && page_type != 3)
                 || (encoding != 0 && encoding != 2 && encoding != 8)
                 || ((encoding == 2 || encoding == 8) && dictionary_values.count() <= 0)
                 || page_value_count <= 0
                 || page_value_count > row_count - decoded_value_count
                 || compressed_size < 0 || uncompressed_size < 0
                 || definition_length < 0 || repetition_length != 0
                 || page_header.pos_ > chunk_end
                 || compressed_size > chunk_end - page_header.pos_
                 || uncompressed_size > 64LL * 1024 * 1024
                 || uncompressed_size > column_meta.total_uncompressed_size_ - decoded_uncompressed_size
                 || (0 == page_type && (definition_encoding != 3 || repetition_encoding != 3))) {
        ret = OB_NOT_SUPPORTED;
      } else {
        decoded_uncompressed_size += uncompressed_size;
        const int64_t body_offset = page_header.pos_;
        const int64_t next_page_offset = body_offset + compressed_size;
        const char *body = chunk_data + body_offset;
        int8_t *levels = nullptr;
        char *plain_values = nullptr;
        int64_t plain_values_size = 0;
        if (0 == page_type) {
          // Data Page V1 compresses the complete body, including level data.
          const int64_t body_buffer_size = OB_MAX(1, uncompressed_size);
          char *uncompressed_body = nullptr;
          if (OB_ISNULL(uncompressed_body = static_cast<char *>(scratch_allocator.alloc(body_buffer_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else if (column_meta.codec_ == 1) {
            ret = file_sql_snappy_decompress(body, compressed_size,
                                             uncompressed_body, uncompressed_size);
          } else if (compressed_size != uncompressed_size) {
            ret = OB_INVALID_DATA;
          } else {
            MEMCPY(uncompressed_body, body, uncompressed_size);
          }
          int64_t value_offset = 0;
          if (OB_SUCC(ret) && schema_column.repetition_type_ == 0) {
            if (OB_ISNULL(levels = static_cast<int8_t *>(scratch_allocator.alloc(page_value_count)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else {
              MEMSET(levels, 1, page_value_count);
            }
          } else if (OB_SUCC(ret) && schema_column.repetition_type_ == 1) {
            if (uncompressed_size < 4) {
              ret = OB_INVALID_DATA;
            } else {
              const uint32_t level_size = static_cast<uint8_t>(uncompressed_body[0])
                  | (static_cast<uint32_t>(static_cast<uint8_t>(uncompressed_body[1])) << 8)
                  | (static_cast<uint32_t>(static_cast<uint8_t>(uncompressed_body[2])) << 16)
                  | (static_cast<uint32_t>(static_cast<uint8_t>(uncompressed_body[3])) << 24);
              if (0 == level_size || level_size > static_cast<uint64_t>(uncompressed_size - 4)) {
                ret = OB_INVALID_DATA;
              } else if (OB_FAIL(file_sql_decode_definition_levels(
                             uncompressed_body + 4, level_size, page_value_count,
                             scratch_allocator, levels))) {
              } else {
                value_offset = 4 + level_size;
              }
            }
          } else if (OB_SUCC(ret)) {
            ret = OB_NOT_SUPPORTED;
          }
          if (OB_SUCC(ret)) {
            plain_values = uncompressed_body + value_offset;
            plain_values_size = uncompressed_size - value_offset;
          }
        } else {
          if (schema_column.repetition_type_ == 0) {
            if (definition_length != 0) {
              ret = OB_INVALID_DATA;
            } else if (OB_ISNULL(levels = static_cast<int8_t *>(scratch_allocator.alloc(page_value_count)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else {
              MEMSET(levels, 1, page_value_count);
            }
          } else if (schema_column.repetition_type_ == 1) {
            if (definition_length <= 0 || definition_length > compressed_size) {
              ret = OB_INVALID_DATA;
            } else if (OB_FAIL(file_sql_decode_definition_levels(body, definition_length,
                                                                page_value_count, scratch_allocator, levels))) {
            }
          } else {
            ret = OB_NOT_SUPPORTED;
          }
          const int64_t value_offset = repetition_length + definition_length;
          const int64_t value_compressed_size = compressed_size - value_offset;
          plain_values_size = uncompressed_size - value_offset;
          if (OB_SUCC(ret) && (value_offset < 0 || value_offset > compressed_size
                               || value_offset > uncompressed_size
                               || plain_values_size < 0 || value_compressed_size < 0)) {
            ret = OB_INVALID_DATA;
          }
          const int64_t value_buffer_size = OB_MAX(1, plain_values_size);
          if (OB_SUCC(ret) && OB_ISNULL(plain_values = static_cast<char *>(
                                                  scratch_allocator.alloc(value_buffer_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else if (OB_SUCC(ret) && values_compressed && column_meta.codec_ == 1) {
            ret = file_sql_snappy_decompress(body + value_offset, value_compressed_size,
                                             plain_values, plain_values_size);
          } else if (OB_SUCC(ret) && values_compressed) {
            ret = OB_NOT_SUPPORTED;
          } else if (OB_SUCC(ret) && value_compressed_size != plain_values_size) {
            ret = OB_INVALID_DATA;
          } else if (OB_SUCC(ret)) {
            MEMCPY(plain_values, body + value_offset, plain_values_size);
          }
        }
        if (OB_SUCC(ret) && (encoding == 2 || encoding == 8)) {
          ret = file_sql_decode_dictionary_values(plain_values, plain_values_size,
                                                  page_value_count, levels,
                                                  dictionary_values, column_values);
        } else if (OB_SUCC(ret) && OB_FAIL(file_sql_decode_plain_values(
                       plain_values, plain_values_size, schema_column.physical_type_,
                       page_value_count, levels, value_allocator, column_values))) {
        }
        if (OB_SUCC(ret)) {
          decoded_value_count += page_value_count;
          page_offset = next_page_offset;
        }
      }
    }
    if (OB_SUCC(ret) && (decoded_value_count != row_count || page_offset != chunk_end)) {
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}
} // namespace

ObFileSQLParquetReader::ObFileSQLParquetReader()
    : file_reader_(nullptr), allocator_(nullptr), row_group_allocator_(), scratch_allocator_(),
      column_names_(), column_types_(), schema_columns_(), column_metas_(), row_group_counts_(),
      selected_column_indexes_(), column_values_(), file_size_(0), row_count_(0), current_row_(0),
      current_row_group_idx_(-1), current_row_in_group_(0), current_row_group_count_(0),
      is_inited_(false)
{
}

int ObFileSQLParquetReader::init(ObFileReader &file_reader, ObIAllocator &allocator,
                                const ObIArray<int64_t> *selected_column_indexes)
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_PARQUET_FOOTER_SIZE = 16LL * 1024 * 1024;
  char magic[4];
  char trailer[8];
  char *footer_data = nullptr;
  int64_t read_size = 0;
  column_names_.reset();
  column_types_.reset();
  schema_columns_.reset();
  column_metas_.reset();
  row_group_counts_.reset();
  selected_column_indexes_.reset();
  column_values_.reset();
  row_group_allocator_.reuse();
  scratch_allocator_.reuse();
  file_reader_ = &file_reader;
  allocator_ = &allocator;
  file_size_ = 0;
  row_count_ = current_row_ = 0;
  current_row_group_idx_ = -1;
  current_row_in_group_ = current_row_group_count_ = 0;
  is_inited_ = false;
  if (OB_FAIL(file_reader.get_file_size(file_size_))) {
  } else if (file_size_ < 12) {
    ret = OB_NOT_SUPPORTED;
  } else if (!file_reader.seekable()) {
    ret = OB_NOT_SUPPORTED;
  } else if (OB_FAIL(file_reader.seek(0))) {
  } else {
    if (OB_FAIL(file_reader.readn(magic, sizeof(magic), read_size))) {
    } else if (read_size != sizeof(magic) || 0 != MEMCMP(magic, "PAR1", sizeof(magic))) {
      ret = OB_INVALID_DATA;
    } else {
      if (OB_FAIL(file_reader.seek(file_size_ - sizeof(trailer)))) {
      } else if (OB_FAIL(file_reader.readn(trailer, sizeof(trailer), read_size))) {
      } else if (read_size != sizeof(trailer) || 0 != MEMCMP(trailer + 4, "PAR1", 4)) {
        ret = OB_INVALID_DATA;
      } else {
        const uint32_t footer_size = static_cast<uint8_t>(trailer[0])
            | (static_cast<uint32_t>(static_cast<uint8_t>(trailer[1])) << 8)
            | (static_cast<uint32_t>(static_cast<uint8_t>(trailer[2])) << 16)
            | (static_cast<uint32_t>(static_cast<uint8_t>(trailer[3])) << 24);
        if (footer_size > static_cast<uint64_t>(file_size_ - 12)
            || footer_size > static_cast<uint64_t>(MAX_PARQUET_FOOTER_SIZE)) {
          ret = OB_NOT_SUPPORTED;
        } else if (OB_ISNULL(footer_data = static_cast<char *>(allocator.alloc(OB_MAX(1, footer_size))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_FAIL(file_reader.seek(file_size_ - 8 - footer_size))) {
        } else if (OB_FAIL(file_reader.readn(footer_data, footer_size, read_size))) {
        } else if (read_size != footer_size) {
          ret = OB_INVALID_DATA;
        } else {
          FileSQLCompactReader footer(footer_data, footer_size, 0);
          if (OB_FAIL(file_sql_parse_file_metadata(footer, allocator, schema_columns_, column_metas_,
                                                   row_group_counts_, row_count_))) {
          } else if (schema_columns_.count() < 2) {
            ret = OB_NOT_SUPPORTED;
          } else {
            const int64_t column_count = schema_columns_.count() - 1;
            for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
              const FileSQLParquetSchemaColumn &schema_column = schema_columns_.at(i + 1);
              if (schema_column.child_count_ != 0 || schema_column.name_.empty()) {
                ret = OB_NOT_SUPPORTED;
              } else {
                ObString name;
                if (OB_FAIL(ob_write_string(allocator, schema_column.name_, name, true))) {
                } else if (OB_FAIL(column_names_.push_back(name))) {
                } else if (0 == schema_column.physical_type_) {
                  if (OB_FAIL(column_types_.push_back(OB_FILE_SQL_BOOLEAN))) {}
                } else if (1 == schema_column.physical_type_ || 2 == schema_column.physical_type_) {
                  if (OB_FAIL(column_types_.push_back(OB_FILE_SQL_BIGINT))) {}
                } else if (4 == schema_column.physical_type_ || 5 == schema_column.physical_type_) {
                  if (OB_FAIL(column_types_.push_back(OB_FILE_SQL_DOUBLE))) {}
                } else if (6 == schema_column.physical_type_) {
                  if (OB_FAIL(column_types_.push_back(OB_FILE_SQL_VARCHAR))) {}
                } else {
                  ret = OB_NOT_SUPPORTED;
                }
              }
            }
            if (OB_SUCC(ret) && (column_metas_.count()
                != row_group_counts_.count() * column_names_.count())) {
              ret = OB_INVALID_DATA;
            }
            for (int64_t i = 0; OB_SUCC(ret) && i < column_names_.count(); ++i) {
              if (OB_ISNULL(selected_column_indexes)) {
                if (OB_FAIL(selected_column_indexes_.push_back(i))) {
                }
              }
            }
            for (int64_t i = 0; OB_SUCC(ret) && OB_NOT_NULL(selected_column_indexes)
                                && i < selected_column_indexes->count(); ++i) {
              const int64_t column_idx = selected_column_indexes->at(i);
              if (column_idx < 0 || column_idx >= column_names_.count()) {
                ret = OB_INVALID_ARGUMENT;
              } else if (OB_FAIL(selected_column_indexes_.push_back(column_idx))) {
              }
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) is_inited_ = true;
  return ret;
}

int ObFileSQLParquetReader::load_row_group(const int64_t row_group_idx)
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_PARQUET_COLUMN_CHUNK_SIZE = 64LL * 1024 * 1024;
  static const int64_t MAX_PARQUET_ROW_GROUP_SIZE = 64LL * 1024 * 1024;
  static const int64_t MAX_PARQUET_ROW_GROUP_VALUES = 1000000;
  column_values_.reset();
  row_group_allocator_.reuse();
  scratch_allocator_.reuse();
  current_row_group_count_ = 0;
  current_row_in_group_ = 0;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(file_reader_) || OB_ISNULL(allocator_))) {
    ret = OB_NOT_INIT;
  } else if (row_group_idx < 0 || row_group_idx >= row_group_counts_.count()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int64_t column_count = column_names_.count();
    const int64_t selected_column_count = selected_column_indexes_.count();
    const int64_t group_row_count = row_group_counts_.at(row_group_idx);
    if (group_row_count <= 0 || column_count <= 0
        || group_row_count > MAX_PARQUET_ROW_GROUP_VALUES
        || (selected_column_count > 0
            && group_row_count > MAX_PARQUET_ROW_GROUP_VALUES / selected_column_count)) {
      ret = OB_NOT_SUPPORTED;
    } else {
      int64_t group_uncompressed_size = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < selected_column_count; ++i) {
        scratch_allocator_.reuse();
        const int64_t file_column_idx = selected_column_indexes_.at(i);
        const int64_t meta_idx = row_group_idx * column_count + file_column_idx;
        if (meta_idx < 0 || meta_idx >= column_metas_.count()) {
          ret = OB_INVALID_DATA;
        } else {
          const FileSQLParquetColumnMeta &column_meta = column_metas_.at(meta_idx);
          const int64_t chunk_start = column_meta.dictionary_page_offset_ >= 0
              ? column_meta.dictionary_page_offset_ : column_meta.data_page_offset_;
          if (chunk_start < 4 || chunk_start >= file_size_ || column_meta.compressed_size_ <= 0
              || column_meta.compressed_size_ > MAX_PARQUET_COLUMN_CHUNK_SIZE
              || column_meta.compressed_size_ > file_size_ - chunk_start
              || column_meta.total_uncompressed_size_ <= 0
              || column_meta.total_uncompressed_size_ > MAX_PARQUET_ROW_GROUP_SIZE - group_uncompressed_size) {
            ret = OB_NOT_SUPPORTED;
          } else {
            group_uncompressed_size += column_meta.total_uncompressed_size_;
            char *chunk_data = nullptr;
            int64_t read_size = 0;
            if (OB_ISNULL(chunk_data = static_cast<char *>(scratch_allocator_.alloc(column_meta.compressed_size_)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else if (OB_FAIL(file_reader_->seek(chunk_start))) {
            } else if (OB_FAIL(file_reader_->readn(chunk_data, column_meta.compressed_size_, read_size))) {
            } else if (read_size != column_meta.compressed_size_) {
              ret = OB_INVALID_DATA;
            } else {
              FileSQLParquetColumnMeta local_meta = column_meta;
              local_meta.data_page_offset_ -= chunk_start;
              if (local_meta.dictionary_page_offset_ >= 0) {
                local_meta.dictionary_page_offset_ -= chunk_start;
              }
              if (OB_FAIL(file_sql_decode_parquet_column_chunk(
                      chunk_data, column_meta.compressed_size_, schema_columns_.at(file_column_idx + 1),
                      local_meta, group_row_count, scratch_allocator_, row_group_allocator_,
                      column_values_))) {
              } else if (column_values_.count() != (i + 1) * group_row_count) {
                ret = OB_INVALID_DATA;
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        current_row_group_idx_ = row_group_idx;
        current_row_group_count_ = group_row_count;
      }
    }
  }
  return ret;
}

int ObFileSQLParquetReader::next_row(ObIArray<ObString> &fields)
{
  int ret = OB_SUCCESS;
  fields.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (current_row_ >= row_count_) {
    ret = OB_ITER_END;
  } else {
    if (current_row_group_idx_ < 0 || current_row_in_group_ >= current_row_group_count_) {
      const int64_t next_group_idx = current_row_group_idx_ + 1;
      if (OB_FAIL(load_row_group(next_group_idx))) {
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < selected_column_indexes_.count(); ++i) {
      ret = fields.push_back(column_values_.at(i * current_row_group_count_ + current_row_in_group_));
    }
    if (OB_SUCC(ret)) {
      ++current_row_;
      ++current_row_in_group_;
    }
  }
  return ret;
}

int ObFileSQLParquetReader::rewind()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    current_row_ = 0;
    current_row_group_idx_ = -1;
    current_row_in_group_ = 0;
    current_row_group_count_ = 0;
    column_values_.reset();
    row_group_allocator_.reuse();
    scratch_allocator_.reuse();
  }
  return ret;
}

/**
 * ObRandomFileReader
 */

ObRandomFileReader::ObRandomFileReader(ObIAllocator &allocator)
    : ObFileReader(allocator),
      offset_(0),
      eof_(false),
      is_inited_(false)
{
}

ObRandomFileReader::~ObRandomFileReader()
{
}

int ObRandomFileReader::open(const ObString &filename)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(file_reader_.open(filename.ptr(), false))) {
  } else {
    filename_ = filename;
    offset_ = 0;
    eof_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObRandomFileReader::read(char *buf, int64_t count, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(file_reader_.pread(buf, count, offset_, read_size))) {
  } else if (0 == read_size) {
    eof_ = true;
  } else {
    offset_ += read_size;
  }
  return ret;
}

int ObRandomFileReader::seek(int64_t offset)
{
  offset_ = offset;
  return OB_SUCCESS;
}

int ObRandomFileReader::get_file_size(int64_t &file_size)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    file_size = ::get_file_size(filename_.ptr());
  }
  return ret;
}

/**
 * ObPacketStreamFileReader
 */
class CSMemPoolAdaptor : public obmysql::ObICSMemPool
{
public:
  explicit CSMemPoolAdaptor(ObIAllocator *allocator)
      : allocator_(allocator)
  {}

  virtual ~CSMemPoolAdaptor() {}

  void *alloc(int64_t size) override
  {
    return allocator_->alloc(size);
  }

private:
  ObIAllocator *allocator_;
};

ObPacketStreamFileReader::ObPacketStreamFileReader(ObIAllocator &allocator)
    : ObStreamFileReader(allocator),
      packet_handle_(NULL),
      session_(NULL),
      timeout_ts_(INT64_MAX),
      // OB_MALLOC_MIDDLE_BLOCK_SIZE 64K. one mysql packet is about 16K
      arena_allocator_(allocator, OB_MALLOC_MIDDLE_BLOCK_SIZE),
      cached_packet_(NULL),
      received_size_(0),
      read_size_(0),
      eof_(false)
{
}

ObPacketStreamFileReader::~ObPacketStreamFileReader()
{
  int ret = OB_SUCCESS;

  LOG_INFO("load data local try to receive all packets from client if eof is false", K_(eof));

  // We read all data from client before close the file.
  // We will stop to handle the process while something error.
  // But the client must send all file content to us and the
  // normal SQL processor cannot handle the packets, so we
  // eat all packets with file content.
  
  // We will wait at most 10 seconds if there is no more data come in.
  const int64_t wait_timeout = 10 * 1000000L; // seconds
  timeout_ts_ = ObTimeUtility::current_time() + wait_timeout;
  int64_t last_received_size = received_size_;
  while (!eof_ && OB_SUCC(ret) && !is_killed()
         && ObTimeUtility::current_time() <= timeout_ts_) {
    ret = receive_packet();
    if (received_size_ > last_received_size) {
      last_received_size = received_size_;
      timeout_ts_ = ObTimeUtility::current_time() + wait_timeout;
    }
  }
  LOG_INFO("load data local file reader exit", K(ret), K(eof_), K(timeout_ts_), K(ObTimeUtility::current_time()));
  if (!eof_ && OB_NOT_NULL(session_) && OB_NOT_NULL(session_->get_cur_exec_ctx())) {
    session_->get_cur_exec_ctx()->set_need_disconnect(true);
    LOG_WARN("we'll close the connection as we can't read all of the file content", K(eof_));
  }
  const int release_ret = release_packet();
  if (OB_SUCCESS != release_ret) {
  }
  arena_allocator_.reset();
}

int ObPacketStreamFileReader::open(const ObString &filename,
                                   ObIClientPacketChannel &packet_handle,
                                   ObSQLSessionInfo *session,
                                   int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(packet_handle_)) {
    ret = OB_INIT_TWICE;
  } else {

    // in `load data local` request, we should send the filename to client
    obmysql::OMPKLocalInfile filename_packet;
    filename_packet.set_filename(filename);
    if (OB_FAIL(packet_handle.response_packet(filename_packet))) {
    } else if (OB_FAIL(packet_handle.flush_buffer(false /*is_last*/))) {
    } else {
      LOG_INFO("[load data local]send filename to client success", K(filename));
    }

    packet_handle_ = &packet_handle;
    session_       = session;
    timeout_ts_    = timeout_ts;
    received_size_ = 0;
    read_size_     = 0;
    eof_           = false;
    LOG_INFO("[load data local] open socket file reader", K_(timeout_ts));
  }
  return ret;
}

/**
 * As decripted in MySQL/MariaDB document, client send the file content with
 * continous packets and `eof` with an empty packet. Every non-empty packet
 * has the format:
 * -------------------
 * MySQL Packet Header
 * string<EOF>
 * -------------------
 * The notation is "string<EOF>" Strings whose length will be calculated by
 * the packet remaining length.
 */
int ObPacketStreamFileReader::read(char *buf, int64_t count, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cached_packet_) || read_size_ == received_size_) {
    ret = receive_packet();
  }

  const int64_t remain_in_packet = received_size_ - read_size_;
  if (OB_SUCC(ret) && OB_NOT_NULL(cached_packet_) && (!eof_ || remain_in_packet > 0)) {
    read_size = MIN(count, remain_in_packet);
    // LOCAL_INFILE_DATA packets carry no command byte: get_cdata() points at
    // the file-content payload itself and get_clen() is its true length.
    const int64_t packet_offset = cached_packet_->get_clen() - remain_in_packet;
    MEMCPY(buf, cached_packet_->get_cdata() + packet_offset, read_size);
    read_size_ += read_size;
  } else {
    read_size = 0;
  }

  int terminate_ret = OB_SUCCESS;
  if (is_timeout()) {
    ret = OB_TIMEOUT;
  } else if (session_ != NULL && session_->is_terminate(terminate_ret)) {
    // Preserve LOAD DATA's historical SESSION_KILLED result while using the
    // common helper to distinguish QUERY_KILLED from QUERY_DEADLOCKED.
    ret = OB_ERR_SESSION_INTERRUPTED == terminate_ret ? OB_SESSION_KILLED : terminate_ret;
  } else if (!eof_ && read_size == 0) {
    ret = OB_IO_ERROR;
  }
  return ret;
}

int ObPacketStreamFileReader::receive_packet()
{
  int ret = OB_SUCCESS;
  ret = release_packet();

  if (OB_SUCC(ret)) {
    arena_allocator_.reuse();
    CSMemPoolAdaptor mem_pool(&arena_allocator_);

    if (!is_timeout() && !is_killed()) {
      obmysql::ObMySQLPacket *pkt = NULL;
      const int64_t timeout_us = -1 == timeout_ts_
          ? -1
          : MAX(0, timeout_ts_ - ObTimeUtility::current_time());
      ret = packet_handle_->wait_packet(mem_pool, timeout_us, pkt);
      cached_packet_ = static_cast<obmysql::ObMySQLRawPacket *>(pkt);
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(cached_packet_)) {
      const int pkt_len = cached_packet_->get_clen();
      if (0 == pkt_len) { // empty packet
        eof_ = true;
        (void)release_packet();
      } else {
        received_size_ += pkt_len;
      }
    }
  }

  // If anything wrong, we end the reading
  if (OB_FAIL(ret)) {
    eof_ = true;
  }
  return ret;
}

int ObPacketStreamFileReader::release_packet()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(cached_packet_)) {
    ret = packet_handle_->release_packet(cached_packet_);
    cached_packet_ = NULL;
  }
  return ret;
}

bool ObPacketStreamFileReader::is_timeout() const
{
  return timeout_ts_ != -1 && ObTimeUtility::current_time() >= timeout_ts_;
}

bool ObPacketStreamFileReader::is_killed() const
{
  return NULL != session_ && (session_->is_query_killed() || session_->is_zombie());
}

/**
 * ObDecompressor
 */
ObDecompressor::ObDecompressor(ObIAllocator &allocator)
    : allocator_(allocator)
{}

ObDecompressor::~ObDecompressor()
{
}

int ObDecompressor::create(ObCSVGeneralFormat::ObCSVCompression format,
                           ObIAllocator &allocator,
                           ObDecompressor *&decompressor)
{
  int ret = OB_SUCCESS;

  decompressor = nullptr;

  switch (format) {
    case ObCSVGeneralFormat::ObCSVCompression::NONE: {
      ret = OB_INVALID_ARGUMENT;
    } break;

    case ObCSVGeneralFormat::ObCSVCompression::GZIP:
    case ObCSVGeneralFormat::ObCSVCompression::DEFLATE: {
      decompressor = OB_NEW(ObZlibDecompressor, MEMORY_ATTR, allocator, format);
    } break;

    case ObCSVGeneralFormat::ObCSVCompression::ZSTD: {
      decompressor = OB_NEW(ObZstdDecompressor, MEMORY_ATTR, allocator);
    } break;

    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unsupported compression format", K(format));
    } break;
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(decompressor)) {
    if (OB_FAIL(decompressor->init())) {
      ObDecompressor::destroy(decompressor);
      decompressor = nullptr;
    }
  }

  return ret;
}

void ObDecompressor::destroy(ObDecompressor *decompressor)
{
  if (OB_NOT_NULL(decompressor)) {
    decompressor->destroy();
    OB_DELETE(ObDecompressor, MEMORY_ATTR, decompressor);
  }
}

/**
 * ObDecompressFileReader
 */
const int64_t ObDecompressFileReader::COMPRESSED_DATA_BUFFER_SIZE = 2 * 1024 * 1024;

ObDecompressFileReader::ObDecompressFileReader(ObIAllocator &allocator)
    : ObStreamFileReader(allocator)
{}

ObDecompressFileReader::~ObDecompressFileReader()
{
  if (OB_NOT_NULL(source_reader_)) {
    OB_DELETE(ObFileReader, MEMORY_ATTR, source_reader_);
  }

  if (OB_NOT_NULL(decompressor_)) {
    ObDecompressor::destroy(decompressor_);
  }

  if (OB_NOT_NULL(compressed_data_)) {
    allocator_.free(compressed_data_);
    compressed_data_ = nullptr;
  }
}

int ObDecompressFileReader::open(const ObFileReadParam &param, ObFileReader *source_reader)
{
  int ret = OB_SUCCESS;

  if (param.compression_format_ == ObCSVGeneralFormat::ObCSVCompression::NONE) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObDecompressor::create(param.compression_format_, allocator_, decompressor_))) {
  } else if (OB_ISNULL(compressed_data_ = (char *)allocator_.alloc(COMPRESSED_DATA_BUFFER_SIZE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate buffer.", K(COMPRESSED_DATA_BUFFER_SIZE));
  } else if (FALSE_IT(source_reader_ = source_reader)) {
  }

  return ret;
}

int ObDecompressFileReader::read(char *buf, int64_t capacity, int64_t &read_size)
{
  int ret = OB_SUCCESS;

  read_size = 0;

  if (OB_ISNULL(source_reader_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(buf) || capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (consumed_data_size_ >= compress_data_size_) {
    if (!source_reader_->eof()) {
      ret = read_compressed_data();
    } else {
      eof_ = true;
    }
  }

  if (OB_SUCC(ret) && compress_data_size_ > consumed_data_size_) {
    int64_t consumed_size = 0;
    ret = decompressor_->decompress(compressed_data_ + consumed_data_size_,
                                    compress_data_size_ - consumed_data_size_,
                                    consumed_size,
                                    buf,
                                    capacity,
                                    read_size);
    if (OB_FAIL(ret)) {
    } else {
      consumed_data_size_ += consumed_size;
      uncompressed_size_  += read_size;
    }
  }

  return ret;
}

int ObDecompressFileReader::read_compressed_data()
{
  int ret = OB_SUCCESS;
  char *read_buffer = compressed_data_;
  if (OB_ISNULL(source_reader_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(consumed_data_size_ < compress_data_size_)) {
    // backup data
    const int64_t last_data_size = compress_data_size_ - consumed_data_size_;
    MEMMOVE(compressed_data_, compressed_data_ + consumed_data_size_, last_data_size);
    read_buffer = compressed_data_ + last_data_size;
    consumed_data_size_ = 0;
    compress_data_size_ = last_data_size;
  } else if (consumed_data_size_ == compress_data_size_) {
    consumed_data_size_ = 0;
    compress_data_size_ = 0;
  }

  if (OB_SUCC(ret)) {
    // read data from source reader
    int64_t read_size = 0;
    int64_t capability = COMPRESSED_DATA_BUFFER_SIZE - compress_data_size_;
    ret = source_reader_->read(read_buffer, capability, read_size);
    if (OB_SUCC(ret)) {
      compress_data_size_ += read_size;
    }
  }
  return ret;
}

/**
 * ObZlibDecompressor
 */

ObZlibDecompressor::ObZlibDecompressor(ObIAllocator &allocator,
                                       ObCSVGeneralFormat::ObCSVCompression compression_format)
    : ObDecompressor(allocator), compression_format_(compression_format)
{}

ObZlibDecompressor::~ObZlibDecompressor()
{
  this->destroy();
}

void ObZlibDecompressor::destroy()
{
  if (OB_NOT_NULL(zlib_stream_ptr_)) {
    z_streamp zstream_ptr = static_cast<z_streamp>(zlib_stream_ptr_);
    inflateEnd(zstream_ptr);
    allocator_.free(zlib_stream_ptr_);
    zlib_stream_ptr_ = nullptr;
  }
}

int ObZlibDecompressor::init()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(zlib_stream_ptr_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(zlib_stream_ptr_ = allocator_.alloc(sizeof(z_stream)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed: zlib stream object.", K(sizeof(z_stream)));
  } else {
    z_streamp zstream_ptr = static_cast<z_streamp>(zlib_stream_ptr_);
    zstream_ptr->zalloc   = ob_zlib_alloc;
    zstream_ptr->zfree    = ob_zlib_free;
    zstream_ptr->opaque   = static_cast<voidpf>(&allocator_);
    zstream_ptr->avail_in = 0;
    zstream_ptr->next_in  = Z_NULL;

    int zlib_ret = inflateInit2(zstream_ptr, 32 + MAX_WBITS);
    if (Z_OK != zlib_ret) {
      ret = OB_ERROR;
      LIB_LOG(WARN, "failed to inflateInit2", K(zlib_ret));
    }
  }
  return ret;
}

int ObZlibDecompressor::decompress(const char *src, int64_t src_size, int64_t &consumed_size,
                                   char *dest, int64_t dest_capacity, int64_t &decompressed_size)
{
  int ret = OB_SUCCESS;
  int zlib_ret = Z_OK;
  z_streamp zstream_ptr = nullptr;

  if (OB_ISNULL(zlib_stream_ptr_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(src) || src_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(dest) || dest_capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (FALSE_IT(zstream_ptr = static_cast<z_streamp>(zlib_stream_ptr_))) {
  } else if (zstream_need_reset_) {
    if (Z_OK != (zlib_ret = inflateReset(zstream_ptr))) {
      ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
      LOG_WARN("failed to reset zlib stream", K(zlib_ret));
    } else {
      zstream_need_reset_ = false;
    }
  }

  if (OB_SUCC(ret)) {
    zstream_ptr->avail_in = src_size;
    zstream_ptr->next_in  = (Bytef *)src;

    int64_t last_avail_in  = zstream_ptr->avail_in;
    int64_t last_total_out = zstream_ptr->total_out;
    zstream_ptr->next_out  = reinterpret_cast<Bytef *>(dest);
    zstream_ptr->avail_out = dest_capacity;

    zlib_ret = inflate(zstream_ptr, Z_NO_FLUSH);
    if (Z_OK == zlib_ret || Z_STREAM_END == zlib_ret) {

      consumed_size     = last_avail_in - zstream_ptr->avail_in;
      decompressed_size = zstream_ptr->total_out  - last_total_out;

      if (Z_STREAM_END == zlib_ret) {
        zstream_need_reset_ = true;
      }
    } else {
      ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
      LOG_WARN("failed to decompress", K(zlib_ret));
    }
  }

  return ret;
}

/**
 * ObZstdDecompressor
 */

ObZstdDecompressor::ObZstdDecompressor(ObIAllocator &allocator)
    : ObDecompressor(allocator)
{}

ObZstdDecompressor::~ObZstdDecompressor()
{
  this->destroy();
}

void ObZstdDecompressor::destroy()
{
  using ObZstdWrapper = oceanbase::common::zstd_1_3_8::ObZstdWrapper;

  if (OB_NOT_NULL(zstd_stream_context_)) {
    ObZstdWrapper::free_stream_dctx(zstd_stream_context_);
    zstd_stream_context_ = nullptr;
  }
}

int ObZstdDecompressor::init()
{
  using OB_ZSTD_customMem = oceanbase::common::zstd_1_3_8::OB_ZSTD_customMem;
  using ObZstdWrapper = oceanbase::common::zstd_1_3_8::ObZstdWrapper;

  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(zstd_stream_context_)) {
    ret = OB_INIT_TWICE;
  } else {
    OB_ZSTD_customMem allocator;
    allocator.customAlloc = ob_zstd_malloc;
    allocator.customFree  = ob_zstd_free;
    allocator.opaque      = &allocator_;

    ret = ObZstdWrapper::create_stream_dctx(allocator, zstd_stream_context_);
    if (OB_FAIL(ret)) {
    }
  }

  return ret;
}

int ObZstdDecompressor::decompress(const char *src, int64_t src_size, int64_t &consumed_size,
                                   char *dest, int64_t dest_capacity, int64_t &decompressed_size)
{
  using ObZstdWrapper = oceanbase::common::zstd_1_3_8::ObZstdWrapper;

  int ret = OB_SUCCESS;
  if (OB_ISNULL(zstd_stream_context_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(src) || src_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(dest) || dest_capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    size_t tmp_consumed_size = 0;
    size_t tmp_decompressed_size = 0;
    ret = ObZstdWrapper::decompress_stream(zstd_stream_context_,
                                           src, src_size, tmp_consumed_size,
                                           dest, dest_capacity, tmp_decompressed_size);
    consumed_size = static_cast<int64_t>(tmp_consumed_size);
    decompressed_size = static_cast<int64_t>(tmp_decompressed_size);
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
