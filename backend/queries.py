PRESET_QUERIES = [
    {
        "id": 1,
        "label": "COUNT — all rows",
        "type": "count",
        "sql": "SELECT COUNT(*) FROM logs",
    },
    {
        "id": 2,
        "label": "COUNT — status = 0",
        "type": "count_filter",
        "sql": "SELECT COUNT(*) FROM logs WHERE status = 0",
    },
    {
        "id": 3,
        "label": "SUM — level",
        "type": "sum",
        "sql": "SELECT SUM(level) FROM logs",
    },
    {
        "id": 4,
        "label": "SUM — level where country = 1",
        "type": "sum_filter",
        "sql": "SELECT SUM(level) FROM logs WHERE country = 1",
    },
    {
        "id": 5,
        "label": "AVG — level",
        "type": "avg",
        "sql": "SELECT AVG(level) FROM logs",
    },
    {
        "id": 6,
        "label": "AVG — compound filter (OR)",
        "type": "avg_filter",
        "sql": "SELECT AVG(level) FROM logs WHERE status = 0 OR country = 1",
    },
    {
        "id": 7,
        "label": "GROUP BY — COUNT per country",
        "type": "group_count",
        "sql": "SELECT COUNT(*) FROM logs GROUP BY country",
    },
    {
        "id": 8,
        "label": "GROUP BY — SUM level per status",
        "type": "group_sum",
        "sql": "SELECT SUM(level) FROM logs GROUP BY status",
    },
    {
        "id": 9,
        "label": "GROUP BY — AVG level per country",
        "type": "group_avg",
        "sql": "SELECT AVG(level) FROM logs GROUP BY country",
    },
    {
        "id": 10,
        "label": "AND filter — status = 0 AND country = 2",
        "type": "and_filter",
        "sql": "SELECT COUNT(*) FROM logs WHERE status = 0 AND country = 2",
    },
    {
        "id": 11,
        "label": "NOT filter — NOT status = 1",
        "type": "not_filter",
        "sql": "SELECT COUNT(*) FROM logs WHERE NOT status = 1",
    },
    {
        "id": 12,
        "label": "APPROX_PERCENTILE — p95 of level",
        "type": "approx_percentile",
        "sql": "SELECT APPROX_PERCENTILE(level, 0.95) FROM logs",
    },
]
