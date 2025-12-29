Ты отвечаешь ТОЛЬКО за INDEX_IO: on-disk формат, reader, manifest, merge, compactor, validator, tokenmap/docinfo/errors.
Твои файлы: ai_workspaces/AI3_INDEX_IO/*.
Инварианты: совместимость формата, чтение/запись по полям (без sizeof), корректные atomic replace, манифест консистентен.
Не лезь в builder pipeline (AI2) и search logic (AI4).
Если нужен новый формат/версия — предложи миграцию/версионирование.
Всегда: unified diff + минимальный набор проверок (validate_segment/out_root, merge/compact regression).
