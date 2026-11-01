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

#define USING_LOG_PREFIX SERVER_OMT
#include "ob_ai_service.h"

#include <cstring>

#include "common/mysqlclient/ob_mysql_result.h"
#include "common/mysqlclient/ob_mysql_transaction.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_mod_define.h"
#include "observer/ai_service/ob_ai_service_executor.h"
#include "share/ob_server_struct.h"
#include "share/rc/ob_server_runtime.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/privilege_check/ob_ai_model_priv_util.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
namespace oceanbase
{
namespace omt
{

namespace
{
const int64_t AI_TASK_LEASE_US = 60L * 1000L * 1000L;
const int64_t AI_TASK_RETRY_DELAY_US = 500L * 1000L;

struct ObAiColumnWorkerTask {
  ObAiColumnWorkerTask()
      : task_id_(OB_INVALID_ID), table_id_(OB_INVALID_ID), column_id_(OB_INVALID_ID), retry_count_(0), max_retries_(0),
        task_state_(0), definition_version_(0), current_definition_version_(0), request_id_(), rowkey_predicate_(),
        database_name_(), table_name_(), result_column_name_(), state_column_name_(), request_column_name_(),
        stage_column_name_(), expression_()
  {
  }

  int64_t task_id_;
  int64_t table_id_;
  int64_t column_id_;
  int64_t retry_count_;
  int64_t max_retries_;
  int64_t task_state_;
  int64_t definition_version_;
  int64_t current_definition_version_;
  ObString request_id_;
  ObString rowkey_predicate_;
  ObString database_name_;
  ObString table_name_;
  ObString result_column_name_;
  ObString state_column_name_;
  ObString request_column_name_;
  ObString stage_column_name_;
  ObString expression_;
};

int copy_worker_result_string(ObMySQLResult& result, const int64_t column_idx, ObIAllocator& allocator, ObString& value)
{
  int ret = OB_SUCCESS;
  ObString source;
  if (OB_FAIL(result.get_varchar(column_idx, source))) {
  } else if (OB_FAIL(ob_write_string(allocator, source, value))) {
  }
  return ret;
}

int append_worker_identifier(ObSqlString& sql, const ObString& identifier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("`"))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < identifier.length(); ++i) {
    if ('`' == identifier[i] && OB_FAIL(sql.append("``"))) {
    } else if ('`' != identifier[i] && OB_FAIL(sql.append(identifier.ptr() + i, 1))) {
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(sql.append("`"))) {
  }
  return ret;
}

int append_worker_hex_literal(ObSqlString& sql, const ObString& value)
{
  static const char hex[] = "0123456789abcdef";
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append("0x"))) {
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < value.length(); ++i) {
    const uint8_t byte = static_cast<uint8_t>(value[i]);
    char pair[2] = {hex[byte >> 4], hex[byte & 0x0f]};
    if (OB_FAIL(sql.append(pair, sizeof(pair)))) {
    }
  }
  return ret;
}

int append_worker_string_literal(ObSqlString& sql, const ObString& value)
{
  int ret = sql.append("'");
  for (int64_t i = 0; OB_SUCC(ret) && i < value.length(); ++i) {
    if ('\'' == value[i] && OB_FAIL(sql.append("''"))) {
    } else if ('\'' != value[i] && OB_FAIL(sql.append(value.ptr() + i, 1))) {
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(sql.append("'"))) {
  }
  return ret;
}

// AI COLUMN stores the original expression and a type-specific system prompt
// in a hidden-column comment. Inject the prompt through AI_COMPLETE's config so
// providers send it as a system message, while retaining the user's config.
int append_worker_ai_expression(ObSqlString& sql, const ObString& stored_expr)
{
  int ret = OB_SUCCESS;
  int64_t separator_pos = -1;
  if (stored_expr.length() > 7 && 0 == MEMCMP(stored_expr.ptr(), "Return ", 7)) {
    for (int64_t i = 0; i + 1 < stored_expr.length(); ++i) {
      if (';' == stored_expr[i] && ' ' == stored_expr[i + 1]) {
        separator_pos = i;
        break;
      }
    }
  }
  if (separator_pos < 0) {
    ret = sql.append(stored_expr.ptr(), stored_expr.length());
  } else {
    const ObString system_prompt(separator_pos + 2, stored_expr.ptr());
    const ObString expr(stored_expr.length() - separator_pos - 2,
                        stored_expr.ptr() + separator_pos + 2);
    int64_t open_pos = -1;
    int64_t comma_pos = -1;
    int64_t config_pos = -1;
    int64_t close_pos = -1;
    bool in_quote = false;
    char quote_char = '\0';
    int64_t depth = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.length(); ++i) {
      const char ch = expr[i];
      if (in_quote) {
        if ('\\' == ch && i + 1 < expr.length()) {
          ++i;
        } else if (ch == quote_char) {
          if (i + 1 < expr.length() && expr[i + 1] == quote_char) {
            ++i;
          } else {
            in_quote = false;
          }
        }
      } else if ('\'' == ch || '"' == ch || '`' == ch) {
        in_quote = true;
        quote_char = ch;
      } else if ('(' == ch) {
        ++depth;
        if (open_pos < 0) {
          open_pos = i;
        }
      } else if (')' == ch && depth > 0) {
        --depth;
        if (0 == depth) {
          close_pos = i;
          break;
        }
      } else if (',' == ch && 1 == depth) {
        if (comma_pos < 0) {
          comma_pos = i;
        } else if (config_pos < 0) {
          config_pos = i;
        }
      }
    }
    if (OB_SUCC(ret) && (open_pos < 0 || comma_pos < 0 || close_pos <= comma_pos
                         || (config_pos >= 0 && config_pos >= close_pos))) {
      ret = OB_NOT_SUPPORTED;
    } else if (OB_SUCC(ret)) {
      const int64_t prompt_start = comma_pos + 1;
      const int64_t prompt_end = config_pos >= 0 ? config_pos : close_pos;
      if (OB_FAIL(sql.append(expr.ptr(), prompt_start))) {
      } else if (OB_FAIL(sql.append(expr.ptr() + prompt_start, prompt_end - prompt_start))) {
      } else if (OB_FAIL(sql.append(", JSON_SET(COALESCE("))) {
      } else if (config_pos >= 0) {
        if (OB_FAIL(sql.append(expr.ptr() + config_pos + 1, close_pos - config_pos - 1))) {
        }
      } else if (OB_FAIL(sql.append("JSON_OBJECT()"))) {
      }
      if (OB_SUCC(ret)
          && (OB_FAIL(sql.append("), '$.__ai_column_system_prompt', "))
              || OB_FAIL(append_worker_string_literal(sql, system_prompt))
              || OB_FAIL(sql.append("))"))
              || OB_FAIL(sql.append(expr.ptr() + close_pos + 1,
                                    expr.length() - close_pos - 1)))) {
      }
    }
  }
  return ret;
}

int load_ai_column_task_group(ObMySQLProxy& sql_proxy, ObIAllocator& allocator, const ObAiColumnWorkerTask& first, ObSEArray<ObAiColumnWorkerTask, 8>& tasks)
{
  int ret = OB_SUCCESS;
  ObSqlString select_sql;
  if (OB_FAIL(select_sql.append("SELECT task.task_id, task.table_id, task.column_id, task.retry_count, "
                                "task.max_retries, task.state, task.definition_version, result.schema_version, "
                                "task.rowkey_predicate, task.request_id, "
                                "db.database_name, tbl.table_name, result.column_name, state.column_name, "
                                "request.column_name, stage.column_name, spec.comment "
                                "FROM oceanbase.__all_ai_column_task task "
                                "JOIN oceanbase.__all_table tbl ON tbl.table_id = task.table_id "
                                "JOIN oceanbase.__all_database db ON db.database_id = tbl.database_id "
                                "JOIN oceanbase.__all_column result ON result.table_id = task.table_id "
                                "AND result.column_id = task.column_id "
                                "JOIN oceanbase.__all_column stage ON stage.table_id = task.table_id "
                                "AND stage.column_name = CONCAT('__ai_stage_', task.column_id) "
                                "JOIN oceanbase.__all_column state ON state.table_id = task.table_id "
                                "AND state.column_name = CONCAT('__ai_state_', task.column_id) "
                                "JOIN oceanbase.__all_column request ON request.table_id = task.table_id "
                                "AND request.column_name = CONCAT('__ai_request_', task.column_id) "
                                "JOIN oceanbase.__all_column spec ON spec.table_id = task.table_id "
                                "AND spec.column_name = CONCAT('__ai_spec_', task.column_id, '_', task.max_retries) "
                                "WHERE task.table_id = "))
      || OB_FAIL(select_sql.append_fmt("%ld AND task.request_id = ", first.table_id_)) || OB_FAIL(append_worker_hex_literal(select_sql, first.request_id_))) {
  } else {
    ObMySQLProxy::MySQLResult result;
    ObMySQLResult* mysql_result = nullptr;
    if (OB_FAIL(sql_proxy.read(result, select_sql.ptr(), 0))) {
    } else if (OB_ISNULL(mysql_result = result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      int next_ret = OB_SUCCESS;
      while (OB_SUCC(ret) && OB_SUCCESS == (next_ret = mysql_result->next())) {
        ObAiColumnWorkerTask task;
        if (OB_FAIL(mysql_result->get_int(static_cast<int64_t>(0), task.task_id_)) || OB_FAIL(mysql_result->get_int(static_cast<int64_t>(1), task.table_id_))
            || OB_FAIL(mysql_result->get_int(2, task.column_id_)) || OB_FAIL(mysql_result->get_int(3, task.retry_count_))
            || OB_FAIL(mysql_result->get_int(4, task.max_retries_)) || OB_FAIL(mysql_result->get_int(5, task.task_state_))
            || OB_FAIL(mysql_result->get_int(6, task.definition_version_))
            || OB_FAIL(mysql_result->get_int(7, task.current_definition_version_))
            || OB_FAIL(copy_worker_result_string(*mysql_result, 8, allocator, task.rowkey_predicate_))
            || OB_FAIL(copy_worker_result_string(*mysql_result, 9, allocator, task.request_id_))
            || OB_FAIL(copy_worker_result_string(*mysql_result, 10, allocator, task.database_name_))
            || OB_FAIL(copy_worker_result_string(*mysql_result, 11, allocator, task.table_name_))
            || OB_FAIL(copy_worker_result_string(*mysql_result, 12, allocator, task.result_column_name_))
            || OB_FAIL(copy_worker_result_string(*mysql_result, 13, allocator, task.state_column_name_))
            || OB_FAIL(copy_worker_result_string(*mysql_result, 14, allocator, task.request_column_name_))
            || OB_FAIL(copy_worker_result_string(*mysql_result, 15, allocator, task.stage_column_name_))
            || OB_FAIL(copy_worker_result_string(*mysql_result, 16, allocator, task.expression_)) || OB_FAIL(tasks.push_back(task))) {
        }
      }
      if (OB_SUCC(ret) && OB_ITER_END != next_ret) {
        ret = next_ret;
      }
    }
  }
  return ret;
}

int record_ai_column_task_failure(ObMySQLProxy& sql_proxy, const ObAiColumnWorkerTask& task,
                                  const int error_code, const bool force_terminal)
{
  int ret = OB_SUCCESS;
  const bool terminal = force_terminal || task.retry_count_ + 1 > task.max_retries_;
  ObSqlString sql;
  if (OB_FAIL(sql.append("UPDATE oceanbase.__all_ai_column_task SET state = "))
      || OB_FAIL(sql.append(terminal ? "5, " : "3, "))
      || OB_FAIL(sql.append("retry_count = retry_count + 1, "))
      || OB_FAIL(sql.append("eligible_ts = "))
      || (terminal ? OB_FAIL(sql.append("NOW(6), "))
                   : (OB_FAIL(sql.append("DATE_ADD(NOW(6), INTERVAL "))
                      || OB_FAIL(sql.append_fmt("%ld MICROSECOND), ", AI_TASK_RETRY_DELAY_US))))
      || OB_FAIL(sql.append_fmt("last_error = CAST(%d AS CHAR) WHERE task_id = %ld AND request_id = ",
                                error_code, task.task_id_))
      || OB_FAIL(append_worker_hex_literal(sql, task.request_id_))
      || OB_FAIL(sql.append(" AND state IN (0, 1, 3)"))) {
  } else {
    ObMySQLTransaction trans;
    int64_t affected_rows = 0;
    if (OB_FAIL(trans.start(&sql_proxy))) {
    } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
      const int end_ret = trans.end(false);
      UNUSED(end_ret);
    } else if (OB_FAIL(trans.end(true))) {
    }
  }
  return ret;
}

int stage_ai_column_task_result(ObMySQLProxy& sql_proxy, const ObAiColumnWorkerTask& task)
{
  int ret = OB_SUCCESS;
  ObSqlString update_sql;
  ObSqlString task_sql;
  if (OB_FAIL(update_sql.append("UPDATE ")) || OB_FAIL(append_worker_identifier(update_sql, task.database_name_))
      || OB_FAIL(update_sql.append(".")) || OB_FAIL(append_worker_identifier(update_sql, task.table_name_))
      || OB_FAIL(update_sql.append(" SET ")) || OB_FAIL(append_worker_identifier(update_sql, task.stage_column_name_))
      || OB_FAIL(update_sql.append(" = ")) || OB_FAIL(append_worker_ai_expression(update_sql, task.expression_))
      || OB_FAIL(update_sql.append(" WHERE ")) || OB_FAIL(update_sql.append(task.rowkey_predicate_.ptr(), task.rowkey_predicate_.length()))
      || OB_FAIL(update_sql.append(" AND ")) || OB_FAIL(append_worker_identifier(update_sql, task.request_column_name_))
      || OB_FAIL(update_sql.append(" = ")) || OB_FAIL(append_worker_hex_literal(update_sql, task.request_id_))
      || OB_FAIL(task_sql.append("UPDATE oceanbase.__all_ai_column_task SET state = 4, last_error = NULL WHERE task_id = "))
      || OB_FAIL(task_sql.append_fmt("%ld AND request_id = ", task.task_id_))
      || OB_FAIL(append_worker_hex_literal(task_sql, task.request_id_))
      || OB_FAIL(task_sql.append(" AND state IN (0, 1, 3)"))) {
  } else {
    ObMySQLTransaction trans;
    int64_t affected_rows = 0;
    int64_t task_affected = 0;
    if (OB_FAIL(trans.start(&sql_proxy))) {
    } else if (OB_FAIL(trans.write(update_sql.ptr(), affected_rows))) {
      const int end_ret = trans.end(false);
      UNUSED(end_ret);
    } else if (OB_FAIL(trans.write(task_sql.ptr(), task_affected))) {
      const int end_ret = trans.end(false);
      UNUSED(end_ret);
    } else if (task_affected <= 0) {
      ret = OB_ENTRY_NOT_EXIST;
      const int end_ret = trans.end(false);
      UNUSED(end_ret);
    } else if (OB_FAIL(trans.end(true))) {
    }
  }
  return ret;
}

int publish_ai_column_task_group(ObMySQLProxy& sql_proxy, const ObIArray<ObAiColumnWorkerTask>& tasks)
{
  int ret = OB_SUCCESS;
  if (tasks.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObAiColumnWorkerTask& first = tasks.at(0);
    ObSqlString update_sql;
    ObSqlString delete_sql;
    if (OB_FAIL(update_sql.append("UPDATE ")) || OB_FAIL(append_worker_identifier(update_sql, first.database_name_))
        || OB_FAIL(update_sql.append(".")) || OB_FAIL(append_worker_identifier(update_sql, first.table_name_))
        || OB_FAIL(update_sql.append(" SET "))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      const ObAiColumnWorkerTask& task = tasks.at(i);
      if ((i > 0 && OB_FAIL(update_sql.append(", ")))
          || OB_FAIL(append_worker_identifier(update_sql, task.result_column_name_))
          || OB_FAIL(update_sql.append(task.task_state_ == 4 ? " = " : " = NULL"))) {
      } else if (task.task_state_ == 4 && OB_FAIL(append_worker_identifier(update_sql, task.stage_column_name_))) {
      }
      if (OB_SUCC(ret)
          && (OB_FAIL(update_sql.append(", "))
              || OB_FAIL(append_worker_identifier(update_sql, task.state_column_name_))
              || OB_FAIL(update_sql.append(task.task_state_ == 4 ? " = 2, " : " = 3, "))
              || OB_FAIL(append_worker_identifier(update_sql, task.request_column_name_))
              || OB_FAIL(update_sql.append(" = NULL, "))
              || OB_FAIL(append_worker_identifier(update_sql, task.stage_column_name_))
              || OB_FAIL(update_sql.append(" = NULL")))) {
      }
    }
    if (OB_SUCC(ret) && (OB_FAIL(update_sql.append(" WHERE "))
        || OB_FAIL(update_sql.append(first.rowkey_predicate_.ptr(), first.rowkey_predicate_.length())))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      if (OB_FAIL(update_sql.append(" AND ")) || OB_FAIL(append_worker_identifier(update_sql, tasks.at(i).request_column_name_))
          || OB_FAIL(update_sql.append(" = ")) || OB_FAIL(append_worker_hex_literal(update_sql, tasks.at(i).request_id_))) {
      }
    }
    if (OB_SUCC(ret) && (OB_FAIL(delete_sql.append("DELETE FROM oceanbase.__all_ai_column_task WHERE table_id = "))
        || OB_FAIL(delete_sql.append_fmt("%ld AND request_id = ", first.table_id_))
        || OB_FAIL(append_worker_hex_literal(delete_sql, first.request_id_)))) {
    }
    if (OB_SUCC(ret)) {
      ObMySQLTransaction trans;
      int64_t affected_rows = 0;
      if (OB_FAIL(trans.start(&sql_proxy))) {
      } else if (OB_FAIL(trans.write(update_sql.ptr(), affected_rows))) {
        const int end_ret = trans.end(false);
        UNUSED(end_ret);
      } else if (OB_FAIL(trans.write(delete_sql.ptr(), affected_rows))) {
        const int end_ret = trans.end(false);
        UNUSED(end_ret);
      } else if (OB_FAIL(trans.end(true))) {
      }
    }
  }
  return ret;
}

int process_one_ai_column_task_group()
{
  int ret = OB_SUCCESS;
  ObMySQLProxy* sql_proxy = GCTX.sql_proxy_;
  ObArenaAllocator allocator(ObMemAttr("AiColumnTask", ObCtxIds::DEFAULT_CTX_ID));
  ObAiColumnWorkerTask first;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObSqlString select_sql;
    if (OB_FAIL(select_sql.append("SELECT task.task_id, task.table_id, task.request_id, task.state "
                                  "FROM oceanbase.__all_ai_column_task task "
                                  "WHERE (task.state IN (0, 1, 3) AND task.eligible_ts <= NOW(6) "
                                  "AND NOT EXISTS (SELECT 1 FROM oceanbase.__all_ai_column_task active "
                                  "WHERE active.table_id = task.table_id AND active.request_id = task.request_id "
                                  "AND active.state IN (0, 1, 3) AND active.eligible_ts > NOW(6))) "
                                  "OR (task.state IN (4, 5) AND NOT EXISTS "
                                  "(SELECT 1 FROM oceanbase.__all_ai_column_task pending "
                                  "WHERE pending.table_id = task.table_id AND pending.request_id = task.request_id "
                                  "AND pending.state NOT IN (4, 5))) "
                                  "ORDER BY task.task_id LIMIT 1"))) {
    } else {
      ObMySQLProxy::MySQLResult result;
      ObMySQLResult* mysql_result = nullptr;
      if (OB_FAIL(sql_proxy->read(result, select_sql.ptr(), 0))) {
      } else if (OB_ISNULL(mysql_result = result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(mysql_result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        }
      } else if (OB_FAIL(mysql_result->get_int(static_cast<int64_t>(0), first.task_id_))
                 || OB_FAIL(mysql_result->get_int(static_cast<int64_t>(1), first.table_id_))
                 || OB_FAIL(copy_worker_result_string(*mysql_result, 2, allocator, first.request_id_))
                 || OB_FAIL(mysql_result->get_int(3, first.task_state_))) {
      }
    }
  }

  // Claim every column in this request group. A short lease lets another
  // observer recover work if this process exits while an AI endpoint is slow.
  if (OB_SUCC(ret) && first.task_state_ != 4 && first.task_state_ != 5) {
    ObMySQLTransaction claim_trans;
    int64_t affected_rows = 0;
    ObSqlString claim_sql;
    if (OB_FAIL(claim_sql.append("UPDATE oceanbase.__all_ai_column_task SET state = 1, "))
        || OB_FAIL(claim_sql.append_fmt("eligible_ts = DATE_ADD(NOW(6), INTERVAL %ld MICROSECOND) ", AI_TASK_LEASE_US))
        || OB_FAIL(claim_sql.append_fmt("WHERE table_id = %ld AND request_id = ", first.table_id_))
        || OB_FAIL(append_worker_hex_literal(claim_sql, first.request_id_)) || OB_FAIL(claim_sql.append(" AND state IN (0, 1, 3) AND eligible_ts <= NOW(6)"))) {
    } else if (OB_FAIL(claim_trans.start(sql_proxy))) {
    } else if (OB_FAIL(claim_trans.write(claim_sql.ptr(), affected_rows))) {
      int end_ret = claim_trans.end(false);
      UNUSED(end_ret);
    } else if (OB_FAIL(claim_trans.end(affected_rows > 0))) {
    } else if (affected_rows <= 0) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }

  ObSEArray<ObAiColumnWorkerTask, 8> tasks;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(load_ai_column_task_group(*sql_proxy, allocator, first, tasks))) {
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
    ObAiColumnWorkerTask& task = tasks.at(i);
    if (task.task_state_ == 4 || task.task_state_ == 5) {
      // A successful result is staged privately; a terminal failure waits for group publication.
    } else if (task.definition_version_ != task.current_definition_version_) {
      if (OB_FAIL(record_ai_column_task_failure(*sql_proxy, task, OB_SCHEMA_ERROR, true))) {
      } else {
        task.task_state_ = 5;
      }
    } else {
      const int task_ret = stage_ai_column_task_result(*sql_proxy, task);
      if (OB_SUCCESS == task_ret) {
        task.task_state_ = 4;
      } else if (OB_ENTRY_NOT_EXIST == task_ret) {
        // A newer DML or a delete removed this request while the model call was in flight.
      } else if (OB_FAIL(record_ai_column_task_failure(*sql_proxy, task, task_ret, false))) {
      } else {
        task.task_state_ = task.retry_count_ + 1 > task.max_retries_ ? 5 : 3;
      }
    }
  }

  bool group_terminal = !tasks.empty();
  for (int64_t i = 0; group_terminal && i < tasks.count(); ++i) {
    group_terminal = tasks.at(i).task_state_ == 4 || tasks.at(i).task_state_ == 5;
  }
  if (OB_SUCC(ret) && group_terminal) {
    if (OB_FAIL(publish_ai_column_task_group(*sql_proxy, tasks))) {
    }
  }
  return ret;
}
} // namespace

const int64_t ObAiColumnTaskWorker::SCHEDULE_INTERVAL_US;

ObAiColumnTaskWorker::ObAiColumnTaskWorker() : is_inited_(false), fail_count_(0) {}

int ObAiColumnTaskWorker::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    is_inited_ = true;
    fail_count_ = 0;
  }
  return ret;
}

int ObAiColumnTaskWorker::start(ObTimer& timer)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!timer.task_exist(*this) && OB_FAIL(timer.schedule(*this, SCHEDULE_INTERVAL_US, true))) {
  }
  return ret;
}

void ObAiColumnTaskWorker::stop(ObTimer& timer)
{
  if (timer.inited() && timer.task_exist(*this)) {
    int ret = timer.cancel_task(*this);
    if (OB_SUCCESS != ret) {
      LOG_WARN("failed to cancel AI column task worker", K(ret));
    }
    ret = timer.wait_task(*this);
    if (OB_SUCCESS != ret) {
      LOG_WARN("failed to wait AI column task worker", K(ret));
    }
  }
}

void ObAiColumnTaskWorker::destroy()
{
  is_inited_ = false;
  fail_count_ = 0;
}

void ObAiColumnTaskWorker::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!share::server_is_write_enabled()) {
    // Only the writable server schedules AI column work.
  } else if (OB_ENTRY_NOT_EXIST == (ret = process_one_ai_column_task_group())) {
    ret = OB_SUCCESS;
    fail_count_ = 0;
  } else if (OB_FAIL(ret)) {
    if (0 == (++fail_count_ % 20)) {
      LOG_WARN("AI column task worker failed; it will retry", K(ret), K_(fail_count));
    }
  } else {
    fail_count_ = 0;
  }
}

ObAiService::ObAiService() : is_inited_(false), timer_(), task_worker_() {}

ObAiService::~ObAiService() { destroy(); }

int ObAiService::server_module_init(ObAiService*& ai_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ai_service)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ai_service->init())) {
  }
  return ret;
}

int ObAiService::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(task_worker_.init())) {
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObAiService::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!timer_.inited() && OB_FAIL(timer_.init("AiColumnTask", ObMemAttr("AiColumnTask")))) {
  } else if (OB_FAIL(timer_.start())) {
  } else if (OB_FAIL(task_worker_.start(timer_))) {
    timer_.stop();
    timer_.wait();
  }
  return ret;
}

void ObAiService::stop()
{
  if (timer_.inited()) {
    task_worker_.stop(timer_);
    timer_.stop();
  }
}

void ObAiService::wait()
{
  if (timer_.inited()) {
    timer_.wait();
  }
}

void ObAiService::destroy()
{
  stop();
  wait();
  timer_.destroy();
  task_worker_.destroy();
  is_inited_ = false;
}

int ObAiService::get_ai_service_guard(ObAiServiceGuard& ai_service_guard)
{
  // TODO: implement after cache support
  int ret = OB_SUCCESS;
  return ret;
}

ObAiServiceGuard::ObAiServiceGuard() : local_allocator_(ObMemAttr("AiServiceGuard", ObCtxIds::DEFAULT_CTX_ID)) {}

ObAiServiceGuard::~ObAiServiceGuard() {}

int ObAiServiceGuard::check_access_privilege()
{
  int ret = OB_SUCCESS;
  bool has_priv = false;

  sql::ObSQLSessionInfo* session = nullptr;
  if (OB_ISNULL(session = THIS_WORKER.get_session())) {
    // system background task thread do not have session, skip check
  } else {
    ObArenaAllocator tmp_allocator;
    share::schema::ObSchemaGetterGuard* schema_guard = session->get_cur_exec_ctx()->get_sql_ctx()->schema_guard_;
    if (OB_ISNULL(schema_guard)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      sql::ObAIServiceEndpointPrivUtil priv_util(*schema_guard);
      share::schema::ObSessionPrivInfo session_priv;
      uint64_t user_id = session->get_priv_user_id();
      if (user_id == OB_INVALID_ID) {
        user_id = OB_SYS_USER_ID;
      }
      if (OB_FAIL(schema_guard->get_session_priv_info(user_id, session->get_database_name(), session_priv))) {
      } else if (OB_FAIL(priv_util.check_access_ai_model_priv(tmp_allocator, session_priv, has_priv))) {
      } else if (!has_priv) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_WARN("no privilege for ai model access", K(ret));
        LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "access ai model");
      }
    }
  }

  return ret;
}

int ObAiServiceGuard::get_ai_endpoint(const common::ObString& name, const share::ObAiModelEndpointInfo*& endpoint_info)
{
  int ret = OB_SUCCESS;
  ObAiModelEndpointInfo* tmp_endpoint_info = nullptr;

  if (OB_FAIL(check_access_privilege())) {
  } else if (OB_ISNULL(tmp_endpoint_info = OB_NEWx(ObAiModelEndpointInfo, &local_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(ObAiServiceExecutor::read_ai_endpoint(local_allocator_, name, *tmp_endpoint_info))) {
  } else {
    endpoint_info = tmp_endpoint_info;
  }
  return ret;
}

int ObAiServiceGuard::get_ai_endpoint_by_ai_model_name(const common::ObString& ai_model_name, const share::ObAiModelEndpointInfo*& endpoint_info,
                                                       bool need_check)
{
  int ret = OB_SUCCESS;
  ObAiModelEndpointInfo* tmp_endpoint_info = nullptr;

  if (need_check && OB_FAIL(check_access_privilege())) {
    LOG_WARN("failed to check access privilege", K(ret));
  } else if (OB_ISNULL(tmp_endpoint_info = OB_NEWx(ObAiModelEndpointInfo, &local_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(ObAiServiceExecutor::read_ai_endpoint_by_ai_model_name(local_allocator_, ai_model_name, *tmp_endpoint_info))) {
  } else {
    endpoint_info = tmp_endpoint_info;
  }
  return ret;
}

} // namespace omt
} // namespace oceanbase
