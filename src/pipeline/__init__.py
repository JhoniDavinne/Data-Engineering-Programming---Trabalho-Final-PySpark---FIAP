"""Pacote de orquestração do pipeline.

Importações eager foram removidas para evitar inicialização prematura do Spark
e risco de importação circular. Use os módulos diretamente:

    from pipeline.cli import run_pipeline
    from pipeline.pipeline_orchestrator import PipelineOrchestrator

Não há importações neste ``__init__``, portanto ``from pipeline import X``
resultaria em ``ImportError``. Importe sempre pelo submódulo completo.
"""
