"""
This is a boilerplate pipeline 'process'
generated using Kedro 0.18.14
"""

from kedro.pipeline import Pipeline, pipeline, node
from . import nodes


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=nodes.pdf_extraction,
                inputs=["pdfs_tables", "pdfs_text"],
                outputs=[
                    "titles",
                    "summary_tables",
                    "stage_tables",
                    "weekdays_freq_tables",
                ],
                name="pdf_extraction",
            ),
            node(
                func=nodes.parse_summary_tables,
                inputs="summary_tables",
                outputs=["parsed_summary_tables", "not_parsable_summary_tables"],
                name="parse_summary_tables",
            ),
            node(
                func=nodes.parse_stage_tables,
                inputs="stage_tables",
                outputs=["parsed_stage_tables", "not_parsable_stage_tables"],
                name="parse_stage_tables",
            ),
            node(
                func=nodes.parse_weekdays_freq_tables,
                inputs=["weekdays_freq_tables", "parsed_summary_tables"],
                outputs=["parsed_weekdays_freq_tables", "not_parsable_weekdays_freq_tables"],
                name="parse_weekdays_freq_tables",
            )
        ]
    )
