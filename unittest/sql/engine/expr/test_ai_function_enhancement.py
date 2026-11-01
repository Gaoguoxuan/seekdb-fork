# Copyright (c) 2026 OceanBase.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied.

"""SQL integration regressions against a local, deterministic HTTP service.

Run with PyMySQL installed and SEEKDB_TEST_PORT pointing at a disposable server.
The service derives responses from the actual inputs; no online models are used.
"""
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import unittest
import uuid

import pymysql


class Handler(BaseHTTPRequestHandler):
    requests = []

    def log_message(self, *args):
        pass

    def do_POST(self):
        value = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.requests.append((self.path, value))
        if self.path == "/array-response":
            result = []
        elif self.path in ("/complete", "/empty-complete"):
            text = value["messages"][-1]["content"] if self.path == "/complete" else ""
            result = {"choices": [{"message": {"content": text}}]}
        elif self.path == "/api/generate":
            if value.get("stream") is not False or self.headers.get("Content-Type") != "application/json":
                self.send_error(400)
                return
            result = {"response": value["prompt"], "done": True}
        elif self.path == "/rerank":
            result = {"results": [{"index": i, "relevance_score": len(text)}
                                  for i, text in enumerate(value["documents"])]}
        else:
            inputs = value["input"]
            dimensions = value.get("dimensions", 4)
            vectors = [[float(len(text)), *[float(i) / 10 for i in range(1, dimensions)]] for text in inputs]
            result = {"embeddings": vectors} if self.path == "/api/embed" else {
                "data": [{"index": i, "embedding": vectors[i]} for i in reversed(range(len(inputs)))]}
            if self.path == "/bad-embedding":
                result = {"data": [{"index": 0, "embedding": ["invalid"]}]}
        data = json.dumps(result, ensure_ascii=False).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class AIFunctionIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = HTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.connection = pymysql.connect(host="127.0.0.1", port=int(os.environ.get("SEEKDB_TEST_PORT", "12882")),
                                         user="root", charset="utf8mb4", autocommit=True, read_timeout=30)
        cls.prefix = "ai_reg_" + uuid.uuid4().hex[:12]
        cls.models = []
        cls.sql("CREATE DATABASE `" + cls.prefix + "`")
        cls.sql("USE `" + cls.prefix + "`")

    @classmethod
    def tearDownClass(cls):
        for model in reversed(cls.models):
            cls.sql("CALL DBMS_AI_SERVICE.DROP_AI_MODEL_ENDPOINT(%s)", (model + "_ep",))
            cls.sql("CALL DBMS_AI_SERVICE.DROP_AI_MODEL(%s)", (model,))
        cls.sql("DROP DATABASE `" + cls.prefix + "`")
        cls.connection.close()
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)

    @classmethod
    def sql(cls, statement, args=None):
        with cls.connection.cursor() as cursor:
            cursor.execute(statement, args)
            return cursor.fetchall() if cursor.description else ()

    def model(self, kind, path, provider="openai"):
        name = self.prefix + "_" + str(len(self.models))
        self.sql("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s,%s)",
                 (name, json.dumps({"type": kind, "model_name": "integration-model"})))
        self.sql("CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s,%s)",
                 (name + "_ep", json.dumps({"ai_model_name": name, "scope": "all", "provider": provider,
                                            "access_key": "" if provider.lower() == "ollama" else "mock-key",
                                            "url": f"http://127.0.0.1:{self.server.server_port}{path}"})))
        self.models.append(name)
        return name

    def test_prompt_preserves_json_and_reads_lobs(self):
        prompt = json.loads(self.sql("SELECT AI_PROMPT('{0}/{0}/{1}',JSON_ARRAY(1,true),JSON_OBJECT('k',2))")[0][0])
        self.assertEqual(prompt["args"], [[1, True], {"k": 2}])
        self.sql("CREATE TABLE prompt_lob(t LONGTEXT,a LONGTEXT)")
        self.sql("INSERT INTO prompt_lob VALUES(%s,%s)", ("{0}-ok", "海洋" * 12000))
        prompt = json.loads(self.sql("SELECT AI_PROMPT(t,a) FROM prompt_lob")[0][0])
        self.assertEqual(prompt, {"template": "{0}-ok", "args": ["海洋" * 12000]})

    def test_complete_repeated_nested_prompt_and_json_config(self):
        model = self.model("completion", "/complete")
        value = self.sql("SELECT AI_COMPLETE(%s,AI_PROMPT('{0}{0}{0}{0}',REPEAT('x',4000)),NULL)", (model,))[0][0]
        self.assertEqual(value, "x" * 16000)
        value = self.sql("SELECT AI_COMPLETE(%s,AI_PROMPT('outer {0}',AI_PROMPT('inner {0}','海洋')),"
                         "JSON_OBJECT('temperature',0.2,'max_tokens',16))", (model,))[0][0]
        self.assertEqual(value, "outer inner 海洋")
        self.assertEqual(Handler.requests[-1][1]["max_tokens"], 16)
        self.assertAlmostEqual(Handler.requests[-1][1]["temperature"], 0.2)
        with self.assertRaises(pymysql.MySQLError):
            self.sql("SELECT AI_COMPLETE(%s,AI_PROMPT('{999999999999999999999999999999}','x'))", (model,))

    def test_embed_batch_maps_rows_and_skips_nulls(self):
        model = self.model("dense_embedding", "/embeddings")
        self.sql("CREATE TABLE embed_rows(id INT PRIMARY KEY,body TEXT)")
        self.sql("INSERT INTO embed_rows VALUES(1,'a'),(2,NULL),(3,'ccc')")
        start = len(Handler.requests)
        rows = self.sql("SELECT id,AI_EMBED(%s,body,3) FROM embed_rows ORDER BY id", (model,))
        self.assertEqual([(row[0], None if row[1] is None else json.loads(row[1])) for row in rows],
                         [(1, [1.0, 0.1, 0.2]), (2, None), (3, [3.0, 0.1, 0.2])])
        calls = Handler.requests[start:]
        self.assertEqual(len(calls), 1)
        self.assertCountEqual(calls[0][1]["input"], ["a", "ccc"])

    def test_bad_model_responses_fail_without_crashing(self):
        embed = self.model("dense_embedding", "/bad-embedding")
        complete = self.model("completion", "/empty-complete")
        array_response = self.model("completion", "/array-response")
        for statement, model in (("SELECT AI_EMBED(%s,'x')", embed),
                                 ("SELECT AI_COMPLETE(%s,'x')", complete),
                                 ("SELECT AI_COMPLETE(%s,'x')", array_response)):
            with self.assertRaises(pymysql.MySQLError):
                self.sql(statement, (model,))
        self.assertEqual(self.sql("SELECT 1"), ((1,),))

    def test_rerank_all_batches_and_original_object_ids(self):
        model = self.model("rerank", "/rerank", "siliconflow")
        documents = [{"id": f"row-{i}", "text": "x" * (i + 1)} for i in range(41)]
        value = self.sql("SELECT AI_RERANK(%s,'q',%s,'text')", (model, json.dumps(documents)))[0][0]
        self.assertEqual([item["id"] for item in json.loads(value)], [f"row-{i}" for i in reversed(range(41))])
        value = self.sql("SELECT AI_RERANK(%s,'q','[\"same\",\"same\",\"longer\"]',NULL)", (model,))[0][0]
        self.assertEqual([item["index"] for item in json.loads(value)], [2, 0, 1])

    def test_ollama_native_protocol_without_api_key(self):
        complete = self.model("completion", "/api/generate", "Ollama")
        embed = self.model("dense_embedding", "/api/embed", "OLLAMA")
        value = self.sql("SELECT AI_COMPLETE(%s,'native')", (complete,))[0][0]
        self.assertEqual(value, "native")
        value = self.sql("SELECT AI_EMBED(%s,'native')", (embed,))[0][0]
        self.assertEqual(json.loads(value), [6.0, 0.1, 0.2, 0.3])


if __name__ == "__main__":
    unittest.main(verbosity=2)
