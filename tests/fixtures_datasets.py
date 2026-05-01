"""Fixtures de dados de exemplo reutilizáveis entre os testes.

Centraliza o CSV de pedidos e o JSONL de pagamentos usados no fluxo E2E,
evitando duplicação e deixando claro o *significado* de cada linha.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

# CSV com separador ; (como no dataset real). Inclui pedidos de 2024 e 2025.
PEDIDOS_CSV = """\
id_pedido;produto;valor_unitario;quantidade;data_criacao;uf;id_cliente
p1;NOTEBOOK;1500.0;2;2025-03-10T12:00:00;SP;1
p2;CELULAR;1000.0;1;2025-04-15T09:00:00;RJ;2
p3;TABLET;1100.0;3;2025-01-01T00:00:00;MG;3
p4;GELADEIRA;2000.0;2;2024-12-31T23:59:59;SP;4
p5;MONITOR;600.0;4;2025-02-20T08:30:00;RJ;5
p6;HOMETHEATER;500.0;1;2025-02-01T10:00:00;SP;6
"""

# Cada objeto cobre um caso: entra no relatório, excluído por fraude, etc.
PAGAMENTOS_JSONL = [
    {
        "id_pedido": "p1",
        "forma_pagamento": "PIX",
        "valor_pagamento": 3000.0,
        "status": False,
        "data_processamento": "2025-03-10T12:05:00",
        "avaliacao_fraude": {"fraude": False, "score": 0.10},
    },
    {
        "id_pedido": "p2",
        "forma_pagamento": "CARTAO_CREDITO",
        "valor_pagamento": 1000.0,
        "status": False,
        "data_processamento": "2025-04-15T09:05:00",
        "avaliacao_fraude": {"fraude": True, "score": 0.95},
    },
    {
        "id_pedido": "p3",
        "forma_pagamento": "BOLETO",
        "valor_pagamento": 3300.0,
        "status": True,
        "data_processamento": "2025-01-01T00:05:00",
        "avaliacao_fraude": {"fraude": False, "score": 0.05},
    },
    {
        "id_pedido": "p4",
        "forma_pagamento": "PIX",
        # valor_pagamento é o valor do ato de pagamento (dataset de pagamentos),
        # distinto de valor_total calculado do pedido (2000.0 * 2 = 4000.0).
        "valor_pagamento": 1850.0,
        "status": False,
        "data_processamento": "2024-12-31T23:59:59",
        "avaliacao_fraude": {"fraude": False, "score": 0.20},
    },
    {
        "id_pedido": "p5",
        "forma_pagamento": "BOLETO",
        "valor_pagamento": 2400.0,
        "status": False,
        "data_processamento": "2025-02-20T08:35:00",
        "avaliacao_fraude": {"fraude": False, "score": 0.30},
    },
    {
        "id_pedido": "p6",
        "forma_pagamento": "PIX",
        "valor_pagamento": 500.0,
        "status": False,
        "data_processamento": "2025-02-01T10:05:00",
        "avaliacao_fraude": {"fraude": False, "score": 0.15},
    },
]


def gravar_pedidos_e_pagamentos_gzip(pasta: Path) -> tuple[Path, Path]:
    """Grava ``pedidos.csv.gz`` e ``pagamentos.json.gz`` em ``pasta``."""
    pedidos_path = pasta / "pedidos.csv.gz"
    pagamentos_path = pasta / "pagamentos.json.gz"

    with gzip.open(pedidos_path, mode="wt", encoding="utf-8") as fp:
        fp.write(PEDIDOS_CSV)

    with gzip.open(pagamentos_path, mode="wt", encoding="utf-8") as fp:
        for obj in PAGAMENTOS_JSONL:
            fp.write(json.dumps(obj))
            fp.write("\n")

    return pedidos_path, pagamentos_path
