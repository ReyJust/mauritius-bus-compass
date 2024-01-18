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
                func=nodes.pdf_splitting,
                inputs=["routes_pdfs", "params:split_pdf.max_pages"],
                outputs="splitted_pdfs",
                name="pdf_splitting",
            ),
            node(
                func=nodes.pdf_extraction,
                inputs=["pdfs_tables", "pdfs_text"],
                outputs=[
                    "titles",
                    "route_1_20D_sample_summary_tables",
                    "stage_tables",
                    "weekdays_freq_tables",
                ],
                name="pdf_extraction",
            ),
            # node(
            #     func=nodes.parse_summary_tables,
            #     inputs="route_1_20D_sample_summary_tables",
            #     outputs="parsed_summary_tables",
            #     name="parse_summary_tables",
            # ),
            # node(
            #     func=nodes.parse_stage_tables,
            #     inputs="stage_tables",
            #     outputs="parsed_stage_tables",
            #     name="parse_stage_tables",
            # ),
            # node(
            #     func=nodes.parse_weekdays_freq_tables,
            #     inputs="weekdays_freq_tables",
            #     outputs="parsed_weekdays_freq_tables",
            #     name="parse_weekdays_freq_tables",
            # )
        ]
    )
