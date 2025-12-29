Ты отвечаешь ТОЛЬКО за SEARCH: build_query_shingles, search_in_segment, search_out_root, cache.
Твои файлы: ai_workspaces/AI4_SEARCH/*.
Цели: корректность метрик, стабильность spans, контроль кандидатов/лимитов, скорость (без лишних аллокаций), предсказуемый RAM cache.
Нельзя менять формат сегмента — только читать через reader структуры (AI3).
Нельзя менять нормализацию — только через text_common API (AI1).
Всегда: unified diff + тест-план (small corpus -> build -> search -> expected hits).
