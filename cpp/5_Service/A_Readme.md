Ты отвечаешь ТОЛЬКО за SERVICE+META: HTTP роуты, L5Service orchestration, ingest_zip, формирование JSON ответа, sqlite storage, tombstones.
Твои файлы: ai_workspaces/AI5_SERVICE_META/*.
Цели: корректные параметры (normalize, max_bytes), безопасность, стабильные ответы, правильная склейка L1-L4 + L5, правильные offsets/limits по tokmap.
Нельзя менять builder/search/format напрямую — только через handoff note в соответствующий модуль.
Всегда: unified diff + репро через curl/Postman + тест-план.
