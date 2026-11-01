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

#ifndef OCEANBASE_SQL_LOAD_DATA_FILE_READER_H_
#define OCEANBASE_SQL_LOAD_DATA_FILE_READER_H_

#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/file/ob_file.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "query/protocol/ob_client_protocol.h"

namespace oceanbase
{
namespace sql
{

class ObSQLSessionInfo;

struct ObFileReadParam
{
public:
  ObFileReadParam();
  TO_STRING_KV(K_(file_location), K_(filename), K_(timeout_ts));
  
public:
  ObLoadFileLocation file_location_;
  ObString filename_;
  ObCSVGeneralFormat::ObCSVCompression compression_format_;
  ObIClientPacketChannel *packet_handle_;
  ObSQLSessionInfo *session_;
  int64_t timeout_ts_;  // A job always has a deadline and file reading may cost a long time

public:
  static int parse_compression_format(ObString compression_name,
                                      ObString filename,
                                      ObCSVGeneralFormat::ObCSVCompression &compression_format);
};

class ObFileReader
{
public:
  ObFileReader(ObIAllocator &allocator) : allocator_(allocator) {}
  virtual ~ObFileReader() {}

  /**
   * read data from file into the buffer
   *
   * @note read_size equals to 0 does not mean end of file.
   *       You should call `eof` to decide whether end of file.
   *       This is not the same with the system call `read`.
   */
  virtual int  read(char *buf, int64_t count, int64_t &read_size) = 0;

  /**
   * get the file size
   *
   * Stream files may not support this feature.
   */
  virtual int  get_file_size(int64_t &file_size) = 0;

  /**
   * seek to the specific position and the `read` subsequently fetch data from the position
   *
   * You can use `seekable` to check whether this file can read at random position.
   */
  virtual int  seek(int64_t offset) = 0;
  virtual bool seekable() const { return true; }
  virtual int64_t get_offset() const = 0;
  virtual bool eof() const = 0;

  /**
   * read data until we got `count` bytes data or exception occurs
   *
   * This routine calls `read` repeatly until we got `count` bytes
   * data.
   * As usual, the normal `read` try to read data once and return.
   */
  int readn(char *buffer, int64_t count, int64_t &read_size);

  /**
   * A file reader factory
   */
  static int open(const ObFileReadParam &param, ObIAllocator &allocator, ObFileReader *& file_reader);
  static void destroy(ObFileReader *file_reader);

private:
  static int open_decompress_reader(const ObFileReadParam &param,
                                    ObIAllocator &allocator,
                                    ObFileReader *source_reader,
                                    ObFileReader *&file_reader);

protected:
  ObIAllocator &allocator_;
};

// A bounded, forward-only CSV record reader used by FileSQL scans. It keeps
// input in a small fixed-size window and returns fields as views into the
// caller-owned row buffer. The views remain valid until the next call.
class ObFileSQLCSVReader
{
public:
  static const int64_t INPUT_BUFFER_SIZE = 8192;
  static const int64_t INITIAL_ROW_BUFFER_SIZE = 8192;
  static const int64_t MAX_ROW_BUFFER_SIZE = 1024 * 1024;
  ObFileSQLCSVReader();
  ~ObFileSQLCSVReader();
  int init(ObFileReader &file_reader,
           common::ObIAllocator &allocator,
           char *row_buffer,
           const int64_t row_buffer_size,
           const char field_delimiter = ',',
           const char quote_char = '"',
           const char escape_char = '"');
  int next_row(common::ObIArray<common::ObString> &fields);
  int rewind();
  void reuse();

private:
  int read_byte(char &c, bool &is_eof);
  int ensure_record_capacity(const int64_t required_size, const int64_t record_len);
  int read_record(int64_t &record_len);
  int parse_record(const int64_t record_len,
                   common::ObIArray<common::ObString> &fields);

private:
  ObFileReader *file_reader_;
  common::ObIAllocator *allocator_;
  char *input_buffer_;
  int64_t input_pos_;
  int64_t input_len_;
  char *row_buffer_;
  int64_t row_buffer_size_;
  char field_delimiter_;
  char quote_char_;
  char escape_char_;
  bool input_eof_;
  bool is_inited_;
};

enum ObFileSQLType : int8_t
{
  OB_FILE_SQL_NULL = 0,
  OB_FILE_SQL_BIGINT,
  OB_FILE_SQL_DOUBLE,
  OB_FILE_SQL_BOOLEAN,
  OB_FILE_SQL_DATE,
  OB_FILE_SQL_DATETIME,
  OB_FILE_SQL_VARCHAR,
};

class ObFileSQLSchema
{
public:
  ObFileSQLSchema() : column_names_(), column_types_(), column_nullable_() {}
  int infer_csv_schema(ObFileSQLCSVReader &reader,
                       common::ObIAllocator &allocator,
                       const bool has_header,
                       common::ObIArray<common::ObString> &fields,
                       const common::ObString &null_string = common::ObString());
  int infer_jsonl_schema(ObFileSQLCSVReader &reader,
                         common::ObIAllocator &allocator,
                         common::ObIArray<common::ObString> &fields);
  static ObFileSQLType infer_value_type(const common::ObString &value,
                                        const common::ObString &null_string = common::ObString());
  static ObFileSQLType merge_types(const ObFileSQLType left, const ObFileSQLType right);
  static uint64_t calc_file_signature(const uint64_t dev,
                                      const uint64_t ino,
                                      const int64_t size,
                                      const int64_t mtime_sec,
                                      const int64_t mtime_nsec,
                                      const int64_t ctime_sec,
                                      const int64_t ctime_nsec);
  const common::ObIArray<common::ObString> &get_column_names() const { return column_names_; }
  const common::ObIArray<ObFileSQLType> &get_column_types() const { return column_types_; }
  const common::ObIArray<bool> &get_column_nullable() const { return column_nullable_; }
  void reset();

private:
  int add_column_names(common::ObIAllocator &allocator,
                       const common::ObIArray<common::ObString> &fields);
  int append_unique_column_name(common::ObIAllocator &allocator,
                                const common::ObString &name,
                                const int64_t column_idx);
  int make_column_name(common::ObIAllocator &allocator,
                       const common::ObString &name,
                       const int64_t suffix,
                       common::ObString &column_name);

private:
  common::ObSEArray<common::ObString, 16> column_names_;
  common::ObSEArray<ObFileSQLType, 16> column_types_;
  common::ObSEArray<bool, 16> column_nullable_;
};

struct FileSQLParquetSchemaColumn
{
  FileSQLParquetSchemaColumn() : name_(), physical_type_(-1), repetition_type_(0), child_count_(0) {}
  common::ObString name_;
  int32_t physical_type_;
  int32_t repetition_type_;
  int32_t child_count_;
};

struct FileSQLParquetColumnMeta
{
  FileSQLParquetColumnMeta()
      : physical_type_(-1), codec_(-1), value_count_(0), total_uncompressed_size_(0),
        compressed_size_(0), data_page_offset_(-1), dictionary_page_offset_(-1) {}
  int32_t physical_type_;
  int32_t codec_;
  int64_t value_count_;
  int64_t total_uncompressed_size_;
  int64_t compressed_size_;
  int64_t data_page_offset_;
  int64_t dictionary_page_offset_;
};

// The reader loads one bounded row group at a time. It keeps only that group's
// decoded values so scans and rewinds do not materialize the whole file.
class ObFileSQLParquetReader
{
public:
  ObFileSQLParquetReader();
  int init(ObFileReader &file_reader, common::ObIAllocator &allocator,
           const common::ObIArray<int64_t> *selected_column_indexes = nullptr);
  int next_row(common::ObIArray<common::ObString> &fields);
  int rewind();
  const common::ObIArray<common::ObString> &get_column_names() const { return column_names_; }
  const common::ObIArray<ObFileSQLType> &get_column_types() const { return column_types_; }

private:
  int load_row_group(const int64_t row_group_idx);

private:
  ObFileReader *file_reader_;
  common::ObIAllocator *allocator_;
  common::ObArenaAllocator row_group_allocator_;
  common::ObArenaAllocator scratch_allocator_;
  common::ObSEArray<common::ObString, 16> column_names_;
  common::ObSEArray<ObFileSQLType, 16> column_types_;
  common::ObSEArray<FileSQLParquetSchemaColumn, 16> schema_columns_;
  common::ObSEArray<FileSQLParquetColumnMeta, 64> column_metas_;
  common::ObSEArray<int64_t, 16> row_group_counts_;
  common::ObSEArray<int64_t, 16> selected_column_indexes_;
  common::ObSEArray<common::ObString, 64> column_values_;
  int64_t file_size_;
  int64_t row_count_;
  int64_t current_row_;
  int64_t current_row_group_idx_;
  int64_t current_row_in_group_;
  int64_t current_row_group_count_;
  bool is_inited_;
};

/**
 * Stream file that can read sequential only
 */
class ObStreamFileReader : public ObFileReader
{
public:
  ObStreamFileReader(ObIAllocator &allocator): ObFileReader(allocator) {}
  virtual ~ObStreamFileReader() {}

  int  get_file_size(int64_t &file_size) override { return OB_NOT_SUPPORTED; }
  int  seek(int64_t offset) override { return OB_NOT_SUPPORTED; }
  bool seekable() const override { return false; }
};

class ObRandomFileReader : public ObFileReader
{
public:
  ObRandomFileReader(ObIAllocator &allocator);
  virtual ~ObRandomFileReader();

  int  read(char *buf, int64_t count, int64_t &read_size) override;
  int  seek(int64_t offset) override;
  int  get_file_size(int64_t &file_size) override;
  int64_t get_offset() const override { return offset_; }
  bool eof() const override { return eof_; }

  int open(const ObString &filename);
  
private:
  ObString             filename_;
  common::ObFileReader file_reader_;
  int64_t              offset_;
  bool                 eof_;
  bool                 is_inited_;
};

/**
 * A strem file reader whose data source is mysql packets
 * Refer to LOAD DATA LOCAL INFILE for more detail.
 * Read data flow:
 * client send file content through mysql packets
 * (@see PacketStreamFileReader::read) and end with an
 * empty mysql packet.
 */
class ObPacketStreamFileReader : public ObStreamFileReader
{
public:
  ObPacketStreamFileReader(ObIAllocator &allocator);
  virtual ~ObPacketStreamFileReader();
  
  int open(const ObString &filename,
           ObIClientPacketChannel &packet_handle,
           ObSQLSessionInfo *session,
           int64_t timeout_ts);

  int read(char *buf, int64_t count, int64_t &read_size) override;
  int64_t get_offset() const override { return read_size_; }
  bool eof() const override { return eof_; }

private:
  int receive_packet();

  /// The packet read from NIO is cached, so we must release it explicitly
  /// and then we can reuse the resource
  int release_packet();
  
  bool is_timeout() const;
  bool is_killed() const;
  
private:
  ObIClientPacketChannel *packet_handle_; // We use this handle to read packet from client
  ObSQLSessionInfo *session_;
  int64_t timeout_ts_; // The deadline of job
  
  // As we read a packet from client, the NIO store the data into the NIO buffer
  // and allocate an ObPacket by an allocator(arena_allocator_). The ObPacket(cached_packet_)
  // is cached in the memory of allocator.
  // NOTE: arena_allocator only ccache the size is smaller than it's page_size(8K,16K, etc)
  ObArenaAllocator arena_allocator_;
  obmysql::ObMySQLRawPacket *cached_packet_;
  
  int64_t received_size_;  // All data received in bytes
  int64_t read_size_;      // All data has been read in bytes
  bool    eof_;
};

/**
 * base class for stream decompressor
 */
class ObDecompressor
{
public:
  explicit ObDecompressor(ObIAllocator &allocator);
  virtual ~ObDecompressor();

  virtual int  init() = 0;
  virtual void destroy() = 0;
  virtual int  decompress(const char *src, int64_t src_size, int64_t &consumed_size,
                          char *dest, int64_t dest_capacity, int64_t &decompressed_size) = 0;

  virtual ObCSVGeneralFormat::ObCSVCompression compression_format() const = 0;

  static int create(ObCSVGeneralFormat::ObCSVCompression format,
                    ObIAllocator &allocator,
                    ObDecompressor *&decompressor);
  static void destroy(ObDecompressor *decompressor);

protected:
  ObIAllocator &allocator_;
};

/**
 * stream decompress file reader
 */
class ObDecompressFileReader : public ObStreamFileReader
{
public:
  explicit ObDecompressFileReader(ObIAllocator &allocator);
  virtual ~ObDecompressFileReader();

  int open(const ObFileReadParam &param, ObFileReader *source_reader);

public:
  int read(char *buf, int64_t capability, int64_t &read_size) override;
  int64_t get_offset() const override { return uncompressed_size_; }
  bool eof() const override { return eof_; }

private:
  int read_compressed_data();

protected:
  ObFileReader *source_reader_ = nullptr;

  char *  compressed_data_    = nullptr; /// compressed data buffer
  int64_t compress_data_size_ = 0;       /// the valid data size in compressed data buffer
  int64_t consumed_data_size_ = 0;       /// handled buffer size in the compressed data buffer
  int64_t uncompressed_size_  = 0;       /// decompressed size from compressed data

  bool  eof_ = false;

  ObDecompressor *decompressor_ = nullptr;

  static const int64_t COMPRESSED_DATA_BUFFER_SIZE;
};

/**
 * gzip/deflate decompressor
 */
class ObZlibDecompressor : public ObDecompressor
{
public:
  explicit ObZlibDecompressor(ObIAllocator &allocator,
                              ObCSVGeneralFormat::ObCSVCompression compression_format);
  virtual ~ObZlibDecompressor();

  int  init() override;
  void destroy() override;

  int decompress(const char *src, int64_t src_size, int64_t &consumed_size,
                 char *dest, int64_t dest_capacity, int64_t &decompressed_size) override;

  ObCSVGeneralFormat::ObCSVCompression compression_format() const override { return compression_format_; }

private:
  void *zlib_stream_ptr_    = nullptr;
  bool  zstream_need_reset_ = false; // the zstreamptr should be reset if we got Z_STREAM_END

  ObCSVGeneralFormat::ObCSVCompression compression_format_;
};

/**
 * zstd decompressor
 */
class ObZstdDecompressor : public ObDecompressor
{
public:
  explicit ObZstdDecompressor(ObIAllocator &allocator);
  virtual ~ObZstdDecompressor();

  int  init() override;
  void destroy() override;

  int decompress(const char *src, int64_t src_size, int64_t &consumed_size,
                 char *dest, int64_t dest_capacity, int64_t &decompressed_size) override;

  ObCSVGeneralFormat::ObCSVCompression compression_format() const override
  {
    return ObCSVGeneralFormat::ObCSVCompression::ZSTD;
  }
private:
  void *zstd_stream_context_ = nullptr;
};

} // namespace sql
} // namespace oceanbase

#endif  // OCEANBASE_SQL_LOAD_DATA_FILE_READER_H_
