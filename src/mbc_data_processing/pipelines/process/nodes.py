"""
This is a boilerplate pipeline 'process'
generated using Kedro 0.18.14
"""

import re
import pandas as pd
from typing import Dict,Literal, Union
from PyPDF2 import PdfWriter
import logging

log = logging.getLogger(__name__)


def get_page_title(pdf_text):
    # Get the title, and remove the 3 first char which is the page number.
    return pdf_text.split("\n")[0].strip()[3:]


def get_route_no(summary_table) -> list[str]:
    route_no_cell = summary_table.loc[0][0]

    route_no = re.findall(r"(\d{1,3}[a-zA-Z]?)", route_no_cell)

    if len(route_no) == 0:
        print(summary_table)
        raise Exception(f"Failed to parse route no from: {route_no_cell}")

    if "including" in route_no_cell.lower():
        including_nos = re.findall(r"([a-zA-Z])\/([a-zA-Z])", route_no_cell)

        if len(including_nos) == 0:
            raise Exception(
                f"Fail to parse route no with Including keyword: {route_no_cell}"
            )

        route_no.extend([f"{route_no[0]}{no}" for no in including_nos[0]])

    return route_no


class PDFExtractionException(Exception):
    """"""


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
        try:
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

                # Empty page
                if len(page_tables) == 0:
                    continue

                # <TableList n=1> ==> It's the end of another table
                elif len(page_tables) == 1:
                    if not route_no:
                        raise Exception(
                            f"The first pdf page seems to not start with the summary table required to get the route_no. {page_index=}"
                        )

                    # ! We use the 'route_no' just declared in the previous loop since that page is the continuation.
                    for no in route_no:
                        stage_tables[no] = pd.concat(
                            [stage_tables[no], page_tables[0].df], axis=0
                        )

                else:
                    summary_table = page_tables[0].df

                    route_no: list[str] = get_route_no(summary_table)

                    stage_table = page_tables[-1].df  # Always the last

                    page_title = get_page_title(page_text)

                    if len(page_tables) == 3:  # There is a weekday frequency table
                        weekday_freq_table = page_tables[1].df

                    for no in route_no:
                        titles[no] = page_title
                        weekday_freq_tables[no] = weekday_freq_table
                        summary_tables[no] = summary_table
                        stage_tables[no] = stage_table

        except Exception as e:
            raise PDFExtractionException(f"FILE: {filename}, PAGE: {page_index} : {e}")

    return titles, summary_tables, stage_tables, weekday_freq_tables


def parse_summary_tables(summary_tables):
    summary_tables = {k: v() for k, v in summary_tables.items()}

    parsed_summary_tables = {}
    referencing_summary_tables = {}
    not_parsable_summary_tables = {}
    for route_no, summary_table in summary_tables.items():
        try:
            if summary_table.shape[1] == 3 and summary_table["2"][0].startswith(
                "Service provided by buses of"
            ):
                referencing_summary_tables[route_no] = summary_table

                continue

            df = summary_table.drop("0", axis=1)
            df = df.fillna(pd.NA)
            df = df.map(clean_spaces, na_action="ignore")
            df = df.dropna(axis=1, how="all")
            df = df.apply(lambda x: x.str.strip())
            df = set_company_name(df, 0, 0, "company")
            df = arrange_df(df)

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

            df = set_route_no(df, route_no)
            df = df.copy()
            df['direction'] = range(1, len(df) + 1)

            parsed_summary_tables[route_no] = df
        except Exception as e:
            log.warn(f"NOT PARSABLE: summary_tables, {route_no}: {e}")

            not_parsable_summary_tables[route_no] = summary_table

    for route_no, table in referencing_summary_tables.items():
        referencing_to = table["2"][0].split(" ")[-1]

        if referencing_to.isdigit():
            parsed_summary_tables[route_no] = parsed_summary_tables[referencing_to]
        else:
            log.warn(f"FAIL TO GET ROUTE NO IN REFERENCING TABLE, {route_no}")
            not_parsable_summary_tables[route_no] = table

    return (
        pd.concat([parsed_summary_tables[k] for k in parsed_summary_tables]),
        not_parsable_summary_tables,
    )


def set_first_row_as_cols(df):
    df.columns = (
        df.iloc[0].str.lower().str.replace(" ", "_")
    )  # Normalize col names in first row and # Set them as col names
    df = df[1:]  # Pop the first row

    return df


def parse_stage_tables(stage_tables):
    stage_tables = {k: v() for k, v in stage_tables.items()}

    parsed_stage_tables = {}
    not_parsable_stage_tables = {}
    for route_no, stage_table in stage_tables.items():
        try:
            df = stage_table.fillna(pd.NA)
            df = df.map(clean_spaces, na_action="ignore")
            df = df.dropna(axis=1, how="all")
            df = df.apply(lambda x: x.str.strip())
            df = df.apply(lambda x: x.str.strip("."))
            df.iloc[0] = df.iloc[0].apply(lambda x: x.replace("\n", ""))

            df = set_first_row_as_cols(df)

            df = df.rename(
                mapper={
                    "average_journey_times_in_munutes": "average_journey_times_in_minutes"
                },
                axis=1,
            )

            # Split the table 3 - 3 cols.
            direction_1 = df.loc[:, ~df.columns.duplicated(keep="first")]
            direction_2 = df.loc[:, ~df.columns.duplicated(keep="last")]

            # Add Direction 1 - 2 column
            direction_1 = direction_1.copy()
            direction_2 = direction_2.copy()
            direction_1.loc[:, "direction"] = 1
            direction_2.loc[:, "direction"] = 2
            direction_1.loc[:, "route_no"] = route_no
            direction_2.loc[:, "route_no"] = route_no

            # Concat right underneith
            parsed_stage_table = pd.concat([direction_1, direction_2], axis=0)
            parsed_stage_tables[route_no] = parsed_stage_table

        except Exception as e:
            log.warn(f"NOT PARSABLE: stage_tables, {route_no}: {e}")

            not_parsable_stage_tables[route_no] = stage_table

    return (
        pd.concat([parsed_stage_tables[k] for k in parsed_stage_tables], axis=0),
        not_parsable_stage_tables,
    )


def have_static_departures(freq_table: pd.DataFrame)-> pd.DataFrame:
    # Return true if any cell contains count("-") > 1 
    return (freq_table.map(lambda x: str(x).count('-')) > 1).any().any()

def split_time_of_departure(row, col_name):
    start, end = pd.NA, pd.NA

    if row[col_name] is not pd.NA:
        if "-" in row[col_name]:
            start, end = row[col_name].split("-")
        elif "After" in row[col_name]:
            start = row[col_name].split(" ")[-1]
        else:
            # "Off Peak Hours" - "Peak Hours"
            log.warn(f"'split_time_of_departure' Edge case: {row[col_name]}")

    splitted = pd.Series({f"{col_name}_start": start, f"{col_name}_end": end})
    splitted = splitted.map(lambda x: x.strip(), na_action="ignore")
    return splitted

def normalize_weekday_freq_table(freq_table: pd.DataFrame)-> pd.DataFrame:
    df = freq_table.copy()
    df = df.map(lambda x: x.replace("–", "-"), na_action="ignore")
    df = df.map(lambda x: x.replace(" - ", "-"), na_action="ignore")
    if (df.shape[0] <= 2):
        df.loc[-1] = pd.Series({"0": "Time of departure"})
        df = df.sort_index().reset_index(drop=True)

    return df.T

def get_weekday_time_interval(first_last: Union[Literal["1st"], Literal["last"]], direction: Union[Literal[1], Literal[2]], df: pd.DataFrame):
    return df.loc[df['direction'] == 1, f'weekdays_{first_last}_bus'].values[0] if direction in df['direction'].values else ""

def enrich_tods(
    freq_table: dict[str, pd.DataFrame], route_no_summaries: pd.DataFrame):

    if not route_no_summaries.empty:
        freq_table = freq_table.copy()

        freq_table.loc[freq_table['direction'] == 1, 'time_of_departure'] = f"{get_weekday_time_interval('1st', 1, route_no_summaries)}-{get_weekday_time_interval('last', 1, route_no_summaries)}"
        freq_table.loc[freq_table['direction'] == 1, 'time_of_departure'] = f"{get_weekday_time_interval('1st', 2, route_no_summaries)}-{get_weekday_time_interval('last', 2, route_no_summaries)}"



def parse_weekdays_freq_tables(
    weekdays_freq_tables: dict[str, pd.DataFrame], parsed_summary_tables: pd.DataFrame
) -> pd.DataFrame:
    departure_col = "departure_time_window"
    weekdays_freq_tables = {k: v() for k, v in weekdays_freq_tables.items()}

    parsed_weekdays_freq_tables = {}
    fixed_tod_weekdays_freq_tables = {}
    not_parsable_weekdays_freq_tables = {}
    for route_no, freq_table in weekdays_freq_tables.items():
        try:
            df = normalize_weekday_freq_table(freq_table)

            if (have_static_departures(df)):
                fixed_tod_weekdays_freq_tables[route_no] = df
                continue


            df = df.fillna(pd.NA)
            df = df.map(clean_spaces, na_action="ignore")
            df = df.dropna(axis=1, how="all")

            df = df.map(lambda x: x.strip(), na_action="ignore")
            df = df.map(lambda x: x.strip("."), na_action="ignore")
            df.iloc[0] = df.iloc[0].apply(lambda x: x.replace("\n", ""))

            df = set_first_row_as_cols(df)

            df = df.rename(
                {"direction_1": "interval", "direction_2": "interval"}, axis=1
            )
            # Split the table 2 - 2 cols.
            direction_1 = df.loc[:, ~df.columns.duplicated(keep="first")]
            direction_2 = df.loc[:, ~df.columns.duplicated(keep="last")]

            # Add Direction 1 - 2 column
            direction_1 = direction_1.copy()
            direction_2 = direction_2.copy()
            direction_1.loc[:, "direction"] = 1
            direction_2.loc[:, "direction"] = 2
            direction_1.loc[:, "route_no"] = route_no
            direction_2.loc[:, "route_no"] = route_no

            # Concat right underneith
            df = pd.concat([direction_1, direction_2], axis=0).fillna(pd.NA)

            if (df['time_of_departure'].isna().sum() != 0):
                df = enrich_tods(df, parsed_summary_tables.loc[ parsed_summary_tables['route_no'] == route_no ][['weekdays_1st_bus', 'weekdays_last_bus','direction']])


            df = df.rename({"time_of_departure": departure_col}, axis=1)
            df = pd.concat(
                [
                    df,
                    df.apply(
                        lambda row: split_time_of_departure(row, departure_col), axis=1
                    ),
                ],
                axis=1,
            )
            df = df.drop(departure_col, axis=1)


            parsed_weekdays_freq_tables[route_no] = df

        except Exception as e:
            log.warn(f"NOT PARSABLE: weekdays_freq_tables, {route_no}: {e}")

            not_parsable_weekdays_freq_tables[route_no] = freq_table

    return (
        pd.concat(
            [parsed_weekdays_freq_tables[k] for k in parsed_weekdays_freq_tables],
            axis=0,
        ),
        not_parsable_weekdays_freq_tables,
    )


def arrange_df(df):
    df = df.copy()

    df.iloc[0, 0] = "starting_point"  # Set the first as the col name
    df["company"].iloc[0] = "company"  # Set the first as the col name

    df = set_first_row_as_cols(df)

    df = df.drop(1, axis=0)  # Drop the first/last bus row

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
