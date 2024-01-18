"""
This is a boilerplate pipeline 'process'
generated using Kedro 0.18.14
"""

import re
import pandas as pd
from typing import Dict
from PyPDF2 import PdfWriter

def pdf_splitting(documents: Dict[str, callable], max_pages: int):

    splitted_documents_pages = {}

    for fname, callable in documents.items():
        document = callable()
        total_pages = len(document.pages)
        
        # Calculate the number of parts needed
        num_parts = -(-total_pages // max_pages)

        for part in range(num_parts):
            start_page = part * max_pages
            end_page = min((part + 1) * max_pages, total_pages)

            splitted_documents_pages[f"{fname}-{part}"] = document.pages[start_page:end_page]


    return splitted_documents_pages


def get_page_title(pdf_text):
    # Get the title, and remove the 3 first char which is the page number.
    return pdf_text.split("\n")[0].strip()[3:]


def get_route_no(summary_table):
    route_no_cell = summary_table.loc[0][0]

    route_no = re.findall(r"Route\s*(\d+[A-Za-z]*)", route_no_cell)

    if not route_no:
        print(summary_table)
        raise Exception(f"Failed to parse route no from: {route_no_cell}")
    return route_no[0]


def pdf_extraction(tables_documents, text_documents):
    if len(tables_documents) != len(text_documents):
        raise Exception(
            f"Different qty of documents between tables and text: {len(tables_documents)=} vs {len(text_documents)=}"
        )

    titles = {}
    summary_tables = {}
    stage_tables = {}
    weekday_freq_tables = {}

    for filename in text_documents:
        tables_from_pages = tables_documents[filename]()
        text_from_pages = text_documents[filename]()

        if len(tables_from_pages) != len(text_from_pages):
            raise Exception(
                f"Different qty of pages between tables and text: {len(tables_from_pages)=} vs {len(text_from_pages)=}"
            )
        route_no = None

        pg_qty = len(text_from_pages)
        for page_index in range(pg_qty):
            page_tables = tables_from_pages[page_index]
            page_text = text_from_pages[page_index]

            # <TableList n=1> ==> It's the end of another table
            if len(page_tables) == 1:
                if not route_no:
                    raise Exception(
                        f"The first pdf page seems to not start with the summary table required to get the route_no. {page_index=}"
                    )
                
                weekday_freq_tables[route_no] = pd.concat(
                    weekday_freq_tables[route_no], page_tables[1].df
                )

            else:
                weekday_freq_table = None

                if len(page_tables) == 3:  # There is a  weekday frequency table
                    weekday_freq_table = page_tables[1].df

                summary_table = page_tables[0].df  # Always the last

                route_no = get_route_no(summary_table)

                stage_table = page_tables[-1].df

                page_title = get_page_title(page_text)

                titles[route_no] = page_title
                summary_tables[route_no] = summary_table
                stage_tables[route_no] = stage_table
                weekday_freq_tables[route_no] = weekday_freq_table

    return titles, summary_tables, stage_tables, weekday_freq_tables


def parse_summary_tables(summary_tables):
    sep = " "
    after_col_split_names = ["1st_bus", "last_bus"]
    cols_to_split = ["weekdays", "saturdays", "sundays_&_public_holidays"]

    summary_tables = {k: v() for k, v in summary_tables.items()}

    parsed_summary_tables = []
    for route_no, summary_table in summary_tables.items():
        df = summary_table.drop("0", axis=1)
        df = df.fillna(pd.NA)
        df = df.map(clean_spaces, na_action="ignore")
        df = df.apply(lambda x: x.str.strip())
        df = set_company_name(df, 0, 0, "company")
        df = arrange_df(df)

        if df.shape[1] == 8:
            df.columns = [
                "starting_point",
                "weekdays_1st_bus",
                "weekdays_last_bus",
                "saturdays_1st_bus",
                "saturdays_last_bus",
                "sundays_&_public_holidays_1st_bus",
                "sundays_&_public_holidays_last_bus",
                "company",
            ]
        else:
            df = start_end_split(df, sep, after_col_split_names, cols_to_split)

        df = set_route_no(df, route_no)
        parsed_summary_tables.append(df)

    return pd.concat(parsed_summary_tables)


def start_end_split(df, sep, after_col_split_names, cols_to_split):
    df = df.copy()

    for to_split_col in cols_to_split:
        splitted = df[to_split_col].str.split(sep, expand=True)
        splitted.columns = [f"{to_split_col}_{col}" for col in after_col_split_names]

        df.drop(to_split_col, axis=1, inplace=True)

        df = pd.concat([df, splitted], axis=1)

    return df


def arrange_df(df):
    df = df.copy()

    df.iloc[0, 0] = "starting_point"
    df["company"].iloc[0] = "company"
    df.columns = df.iloc[0].str.lower().str.replace(" ", "_")
    df = df[1:]
    df = df.drop(1, axis=0)

    return df


def set_company_name(
    df: pd.DataFrame, x: int, y: int, company_col_name: str
) -> pd.DataFrame:
    df = df.copy()
    company_name = df.iloc[x, y]

    df[company_col_name] = company_name

    return df


def set_route_no(df: pd.DataFrame, route_no: str) -> pd.DataFrame:
    df = df.copy()

    df["route_no"] = route_no
    return df


import re


def clean_spaces(x):
    return re.sub(r"\s{2,}", " ", x)


# def parse_stage_tables(stage_tables):
#     return stage_tables

# def parse_weekdays_freq_tables(weekdays_freq_tables):
#     return weekdays_freq_tables
