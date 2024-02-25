"""
This is a boilerplate pipeline 'process'
generated using Kedro 0.18.14
"""

import re
import pandas as pd
from typing import Dict, Literal, Union
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

    titles = []
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
                        existing_keys = set().union(
                            weekday_freq_tables.keys(),
                            summary_tables.keys(),
                            stage_tables.keys(),
                        )
                        if no in existing_keys:
                            no = f"{no}_bis"
                            log.warn(f"Duplicate Bus Route NO: {no}")

                        titles.append([page_title, no])
                        weekday_freq_tables[no] = weekday_freq_table
                        summary_tables[no] = summary_table
                        stage_tables[no] = stage_table

        except Exception as e:
            raise PDFExtractionException(f"FILE: {filename}, PAGE: {page_index} : {e}")

    return (
        pd.DataFrame(columns=["title", "route_no"], data=titles),
        summary_tables,
        stage_tables,
        weekday_freq_tables,
    )


def parse_titles(titles_df: pd.DataFrame) -> pd.DataFrame:
    df = titles_df.copy()

    df["title"] = df["title"].map(lambda x: x.replace("−", "-"), na_action="ignore")
    df["title"] = df["title"].map(lambda x: x.replace("–", "-"), na_action="ignore")
    df["title"] = df["title"].map(lambda x: x.replace(" - ", "-"), na_action="ignore")

    # TODO: deal with '

    return df


def arrange_df(df):
    df = df.copy()

    df.iloc[0, 0] = "starting_point"  # Set the first as the col name
    df["operator"].iloc[0] = "operator"  # Set the first as the col name

    for column_name in df.columns:
        if df[column_name].iloc[1:].isna().all():
            df = df.drop(column_name, axis=1)

    df = set_first_row_as_cols(df)

    df = df.drop(1, axis=0)  # Drop the first/last bus row

    return df


def process_summary_edge_cases(
    summary_table: pd.DataFrame, route_no: str
) -> pd.DataFrame:
    summary_table = summary_table.copy()

    summary_table = pd.DataFrame(
        columns=[
            "starting_point",
            "weekdays_1st_bus",
            "weekdays_last_bus",
            "saturdays_1st_bus",
            "saturdays_last_bus",
            "sundays_&_public_holidays_1st_bus",
            "sundays_&_public_holidays_last_bus",
            "operator",
        ]
    )

    return summary_table


def process_week_partial_table(df):
    df = df.copy()

    week_part = df.loc[0, "2"]

    for column_name in df.columns:
        if (df[column_name].iloc[1:] == "1 trip").all():
            df = df.drop(column_name, axis=1)

    if all(
        [key in week_part for key in ["Weekdays", "Saturdays", "Sundays", "Holidays"]]
    ):
        print("AAAA")

    elif "Weekdays" in week_part:
        df["4"] = pd.NA
        df["5"] = pd.NA
        df["6"] = pd.NA
        df["7"] = pd.NA
        new_row = pd.DataFrame(
            data={
                "1": [pd.NA],
                "2": ["1st bus"],
                "3": ["Last bus"],
                "4": ["1st bus"],
                "5": ["Last bus"],
                "6": ["1st bus"],
                "7": ["Last bus"],
            }
        )

        df = pd.concat([df.iloc[:1], new_row, df.iloc[1:]]).reset_index(drop=True)
        df = df[["1", "2", "3", "4", "5", "6", "7", "operator"]]

    elif "Holidays" in week_part:
        df = df.rename({"2": "6"}, axis=1)
        df["2"] = pd.NA
        df["3"] = pd.NA
        df["4"] = pd.NA
        df["5"] = pd.NA
        df["7"] = pd.NA
        # new_row = pd.DataFrame(data={'1': [pd.NA], '2': ["1st bus"], '3': ["Last bus"], '4': ["1st bus"], '5': ["Last bus"], '6': ["1st bus"], '7': ["Last bus"]})
        new_row = pd.DataFrame(columns=["1", "2", "3", "4", "5", "6", "7", "operator"])
        new_row.loc[0] = pd.NA

        df = pd.concat([df.iloc[:1], new_row, df.iloc[1:]]).reset_index(drop=True)
        df = df[["1", "2", "3", "4", "5", "6", "7", "operator"]]

    return df


def normalization(df):
    df = df.copy()

    df = df.fillna(pd.NA)

    # Drop all column with only null inside.
    for column_name in df.columns:
        if df[column_name].isna().all():
            df = df.drop(column_name, axis=1)

    df = df.map(lambda x: x.replace("–", "-"), na_action="ignore")
    df = df.map(lambda x: x.replace(" - ", "-"), na_action="ignore")

    return df


def get_minimal_stbl(stbl):
    return pd.DataFrame(
        data={
            "starting_point": [pd.NA],
            "weekdays_1st_bus": [pd.NA],
            "weekdays_last_bus": [pd.NA],
            "saturdays_1st_bus": [pd.NA],
            "saturdays_last_bus": [pd.NA],
            "sundays_&_public_holidays_1st_bus": [pd.NA],
            "sundays_&_public_holidays_last_bus": [pd.NA],
            "operator": [get_operator(stbl, 0, 1)],
        }
    )


def parse_summary_tables(
    summary_tables: Dict[str, pd.DataFrame], edge_cases: list[str]
):
    """
    Syntax: stbl(s) => summary_table(s)
    """

    stbls = {k: v() for k, v in summary_tables.items()}

    parsed_stbls = {}
    referencing_stbls = {}
    not_parsable_stbls = {}
    edge_cases_stbls = {}
    frequency_in_summary = {}

    try:
        edge_cases_stbls = {key: stbls[key] for key in edge_cases}
        [stbls.pop(key) for key in edge_cases]
    except Exception as e:
        raise Exception(
            f"Error when sorting out edge cases. You might have reference an inexisting route number in parameters: {e}"
        )

    standard = (4, 8)

    for route_no, summary_table in stbls.items():

        stbl = normalization(summary_table)
        col_count, row_count = stbl.shape

        if stbl.shape != standard:
            if stbl.apply(lambda x: x.str.contains("-", na=False)).any().any():
                # 1D
                # 207A
                # 210B
                # 216
                # 230
                # 60

                stbl = set_route_no(stbl, route_no)
                stbl = set_operator(stbl, 0, 1, "operator")
                frequency_in_summary[route_no] = stbl

            elif col_count == 1:
                if row_count == 3 and " by buses of route " in stbl["2"][0].lower():
                    # Table that are referencing another bus line.

                    # 103A
                    # 105A
                    # 12A
                    # 13
                    # 13D
                    # 14A
                    # 14B
                    # 15A
                    # 15C
                    # 16D
                    # 17A
                    # 17B
                    # 17D
                    # 17E
                    # 17F
                    # 20B
                    # 20C
                    # 20D
                    # 226B
                    # 245
                    # 252
                    # 28A
                    # 52A
                    # 52C
                    # 52D
                    # 57A
                    # 61A
                    # 65
                    # 66A
                    # 75A
                    # 80A
                    # 89B
                    # 90
                    referencing_stbls[route_no] = stbl

                    continue

                else:
                    # Table that have no data.

                    # 156A
                    # 179
                    # 179A
                    # 246
                    # 248
                    # 5A
                    # 66C
                    # 8B
                    parsed_stbl = get_minimal_stbl(stbl)
                    parsed_stbl = set_route_no(parsed_stbl, route_no)
                    parsed_stbls[route_no] = parsed_stbl

                    continue

            elif col_count > 4:
                # There is more than 1 Start and 1 End
                # 17
                # 17G
                # 207A
                # 89A

                continue

            elif row_count >= 3 and row_count <= 4:
                # Table that only reference weekday, or sunday/holiday, not both.
                # 175
                # 180
                # 181
                # 189
                # 191
                # 202
                # 206A
                # 207B
                # 207C
                # 210B
                # 230
                # 60

                # Handle 1 trip situation
                for column_name in stbl.columns:
                    if (stbl[column_name].iloc[1:] == "1 trip").all():
                        stbl = stbl.drop(column_name, axis=1)

                # Handle more more than one day type referenced in a single col.
                splitted = stbl["2"].str.split(", ", expand=True)
                for col in splitted.columns:
                    splitted[col].iloc[1:] = splitted[0].iloc[1:]

                stbl = stbl.drop("2", axis=1)
                stbl = pd.concat([stbl, splitted], axis=1)
                stbl.columns = [str(i) for i in range(len(stbl.columns))]

                if stbl.shape[1] == 3:
                    # These are fixed schedule for 1 trip buses.
                    stbl = set_route_no(stbl, route_no)
                    stbl = set_operator(stbl, 0, 1, "operator")
                    frequency_in_summary[route_no] = stbl

                # parsed_stbls[route_no] = stbl

            else:
                log.error(f"NOT PARSABLE: summary_tables, {route_no}: {e}")
                not_parsable_stbls[route_no] = stbl

        try:

            if route_no == "180":
                pass

            stbl = stbl.drop("0", axis=1)
            stbl = stbl.fillna(pd.NA)
            stbl = stbl.map(clean_spaces, na_action="ignore")
            stbl = stbl.dropna(axis=1, how="all")
            stbl = stbl.apply(lambda x: x.str.strip())
            stbl = set_operator(stbl, 0, 0, "operator")

            stbl = arrange_df(stbl)
            stbl = set_route_no(stbl, route_no)

            parsed_stbls[route_no] = stbl

        except Exception as e:
            log.warn(f"NOT PARSABLE: summary_tables, {route_no}: {e}")

    # Overwriting the EDGE CASES
    for route_no in edge_cases_stbls:
        edge_cases[route_no] = set_route_no(edge_cases[route_no], route_no)
        parsed_stbls[route_no] = pd.DataFrame(data=edge_cases[route_no])

    # PROCESSING REFERENCING CASES
    # for route_no, stbl in referencing_stbls.items():
    #     reference_to = stbl["2"][0].split(" ")[-1]

    #     if reference_to in parsed_stbls.keys():
    #         parsed_stbls[route_no] = parsed_stbls[referencing_to]
    #     else:
    #         log.warn(f"FAIL TO GET ROUTE NO IN REFERENCING TABLE: {route_no}")
    #         not_parsable_stbls[route_no] = stbl

    # [ k for k in parsed_stbls if len(parsed_stbls[k].columns) != 9 ]

    return (
        pd.concat([parsed_stbls[k] for k in parsed_stbls]),
        not_parsable_stbls,
    )


# def parse_summary_tables(summary_tables: Dict[str, pd.DataFrame], edge_cases: list[str]):
#     summary_tables = {k: v() for k, v in summary_tables.items()}

#     parsed_summary_tables = {}
#     referencing_summary_tables = {}
#     not_parsable_summary_tables = {}
#     for route_no, summary_table in summary_tables.items():
#         try:
#             if summary_table.shape[1] == 3 and summary_table["2"][0].startswith(
#                 "Service provided by buses of"
#             ):
#                 referencing_summary_tables[route_no] = summary_table

#                 continue


#             if route_no in edge_cases:
#                 df = process_summary_edge_cases(summary_table, route_no)
#             else:
#                 if route_no == '245':
#                     print('i')
#                     # 180 Weekdays, Saturdays, Sundays and Public Holidays in a single cell

#                 df = summary_table.drop("0", axis=1)
#                 df = df.fillna(pd.NA)
#                 df = df.map(clean_spaces, na_action="ignore")
#                 df = df.dropna(axis=1, how="all")
#                 df = df.apply(lambda x: x.str.strip())
#                 df = set_operator(df, 0, 0, "operator")

#                 if df.shape[1] <= 4:
#                     df = process_week_partial_table(df)

#                 df = arrange_df(df)

#                 df.columns = [
#                     "starting_point",
#                     "weekdays_1st_bus",
#                     "weekdays_last_bus",
#                     "saturdays_1st_bus",
#                     "saturdays_last_bus",
#                     "sundays_&_public_holidays_1st_bus",
#                     "sundays_&_public_holidays_last_bus",
#                     "operator",
#                 ]

#             df = set_route_no(df, route_no)
#             df = df.copy()
#             df['direction'] = range(1, len(df) + 1)

#             parsed_summary_tables[route_no] = df
#         except Exception as e:
#             log.warn(f"NOT PARSABLE: summary_tables, {route_no}: {e}")

#             not_parsable_summary_tables[route_no] = summary_table

#     for route_no, table in referencing_summary_tables.items():
#         referencing_to = table["2"][0].split(" ")[-1]

#         if referencing_to in parsed_summary_tables.keys():
#             parsed_summary_tables[route_no] = parsed_summary_tables[referencing_to]
#         else:
#             log.warn(f"FAIL TO GET ROUTE NO IN REFERENCING TABLE, {route_no}")
#             not_parsable_summary_tables[route_no] = table

#     return (
#         pd.concat([parsed_summary_tables[k] for k in parsed_summary_tables]),
#         not_parsable_summary_tables,
#     )


def set_first_row_as_cols(df):
    df.columns = (
        df.iloc[0].str.lower().str.replace(" ", "_")
    )  # Normalize col names in first row and # Set them as col names
    df = df[1:]  # Pop the first row

    return df


def keep_only_digit(x: str) -> str:
    return re.search(r"\d+", x).group()

def handle_time_conversions(time):
    time_str = str(time)

    # Dealing with intervals
    if "-" in time_str:
        start, end = time_str.split('-')

        return (float(start) + float(end)) / 2
    
    if 'h' in time_str:
        hours, minutes = time_str.split('h')
        total_minutes = float(hours) * 60
        total_minutes += float(minutes)

        return total_minutes
    else:
        return float(time_str)
    

def is_increasing(df: pd.DataFrame, journey_time_col: str)-> bool:
    the_serie: pd.Series = df[journey_time_col].dropna()

    if the_serie.sum() == 0:
        return True
    
    return all(the_serie.iloc[i] < the_serie.iloc[i+1] for i in range(len(the_serie)-1))

def direction_processing(
    directions_dfs: list[pd.DataFrame], fare_stage_col: str, journey_time_col: str, route_no: str, journey_time_edge_cases
) -> list[pd.DataFrame]:
    direction_dfs_pcd = []

    for idx, df in enumerate(directions_dfs):
        df = df.copy()
        df.loc[:, "direction"] = idx + 1

        df.loc[:, "route_no"] = route_no

        # Fare stage, as nullable integer
        df[fare_stage_col] = df[fare_stage_col].map(keep_only_digit, na_action="ignore").astype("Int64")
        df[journey_time_col] = df[journey_time_col].replace(0.0, pd.NA)
        if pd.isna(df[journey_time_col].loc[1]):
            df.loc[1, journey_time_col] = 0


        try:
            df[journey_time_col] = df[journey_time_col].astype('Float64')
        except Exception as e:

            df[journey_time_col] = df[journey_time_col].map(lambda x: handle_time_conversions(x), na_action='ignore')

            df[journey_time_col] = df[journey_time_col].astype('Float64')


        if not is_increasing(df, journey_time_col):
            direction = df['direction'].loc[1]

            edge_case = journey_time_edge_cases[f"direction_{direction}"].get( route_no, None )

            if edge_case == None:
                # Not cumulative
                df[journey_time_col] = df[journey_time_col].cumsum(skipna=True)
            elif edge_case['action'] == 'skip': # Just inconsistent, no solution
                log.warn(f'Journey time inconsistent in: {route_no}')
                pass
            elif edge_case['action'] == 'set': # PDF wasn't correctly parsed or wrong input at it's creation
                df.loc[ edge_case['idx'], journey_time_col ] = edge_case['value']
            elif edge_case['action'] == 'reverse': #Is upside down
                df[journey_time_col] = df[journey_time_col][::-1]

        direction_dfs_pcd.append(df)

    return direction_dfs_pcd


def parse_stage_tables(stage_tables, stage_edge_cases):
    fare_stage_col = "fare_stage"
    journey_time_col = "average_journey_times_in_minutes"

    journey_time_edge_cases = stage_edge_cases['journey_time']
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
                    "average_journey_times_in_munutes": journey_time_col
                },
                axis=1,
            )

            # Split the table 3 - 3 cols.
            direction_1, direction_2 = (
                df.loc[:, ~df.columns.duplicated(keep="first")],
                df.loc[:, ~df.columns.duplicated(keep="last")],
            )

            direction_1, direction_2 = direction_processing(
                [direction_1, direction_2], fare_stage_col, journey_time_col, route_no, journey_time_edge_cases
            )

            d1_missing_fare_stages, d2_missing_fare_stages = (
                direction_1[fare_stage_col].isna().all(),
                direction_2[fare_stage_col].isna().all(),
            )

            if d1_missing_fare_stages:
                direction_1[fare_stage_col] = direction_1.index + 1

            if d2_missing_fare_stages:
                # * If d1 have the fares, we just upside it down.
                if not d1_missing_fare_stages:
                    direction_2[fare_stage_col] = direction_1[fare_stage_col][::-1]
                else:
                    direction_2[fare_stage_col] = direction_2.index + 1

            # Vertical concat
            parsed_stage_tables[route_no] = pd.concat([direction_1, direction_2], axis=0)

        except Exception as e:
            log.warn(f"NOT PARSABLE: stage_tables, {route_no}: {e}")

            not_parsable_stage_tables[route_no] = stage_table

    # Unify the stage into a single table
    parsed_stage_tables: pd.DataFrame = pd.concat(
        [parsed_stage_tables[k] for k in parsed_stage_tables], axis=0
    )

    return (
        parsed_stage_tables,
        not_parsable_stage_tables,
    )


def have_static_departures(freq_table: pd.DataFrame) -> pd.DataFrame:
    # Return true if any cell contains count("-") > 1
    return (freq_table.map(lambda x: str(x).count("-")) > 1).any().any()


def is_horizontal_static_departures(df, column_name):
    return all(df[column_name] == "Time of departure")


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


def normalize_weekday_freq_table(
    freq_table: pd.DataFrame, route_no: str, edge_cases: list[str]
) -> pd.DataFrame:
    df = freq_table.copy()
    df = df.map(lambda x: x.replace("–", "-"), na_action="ignore")
    df = df.map(lambda x: x.replace(" - ", "-"), na_action="ignore")
    df = df.map(
        lambda x: x.replace("(Additional departures on market days)", ""),
        na_action="ignore",
    )
    if df.shape[0] <= 2:
        if is_horizontal_static_departures(df, "1"):
            df["1"] = df.iloc[:, 2:].apply(lambda row: "-".join(row.dropna()), axis=1)
            df = df.drop(df.columns[2:], axis=1)

        df.loc[-1] = pd.Series({"0": "Time of departure"})
        df = df.sort_index().reset_index(drop=True)

    if route_no in edge_cases:
        df = process_wk_edge_cases(df, route_no)

    return df.T


def process_wk_edge_cases(df: pd.DataFrame, route_no: str) -> pd.DataFrame:
    df = df.copy()

    if route_no == "7":
        df["1"] = df.iloc[:, 1:].apply(lambda row: "-".join(row.dropna()), axis=1)
        df = df.drop(df.columns[2:], axis=1)

    return df


def get_weekday_time_interval(
    first_last: Union[Literal["1st"], Literal["last"]],
    direction: Union[Literal[1], Literal[2]],
    df: pd.DataFrame,
):
    return (
        df.loc[df["direction"] == 1, f"weekdays_{first_last}_bus"].values[0]
        if direction in df["direction"].values
        else ""
    )


def enrich_tods(freq_table: dict[str, pd.DataFrame], route_no_summaries: pd.DataFrame):

    if not route_no_summaries.empty:
        freq_table = freq_table.copy()

        freq_table.loc[freq_table["direction"] == 1, "time_of_departure"] = (
            f"{get_weekday_time_interval('1st', 1, route_no_summaries)}-{get_weekday_time_interval('last', 1, route_no_summaries)}"
        )
        freq_table.loc[freq_table["direction"] == 1, "time_of_departure"] = (
            f"{get_weekday_time_interval('1st', 2, route_no_summaries)}-{get_weekday_time_interval('last', 2, route_no_summaries)}"
        )

    return freq_table


def parse_weekdays_freq_tables(
    weekdays_freq_tables: dict[str, pd.DataFrame],
    parsed_summary_tables: pd.DataFrame,
    edge_cases: list[str],
) -> pd.DataFrame:
    departure_col = "departure_time_window"
    weekdays_freq_tables = {k: v() for k, v in weekdays_freq_tables.items()}

    parsed_weekdays_freq_tables = {}
    not_parsable_weekdays_freq_tables = {}
    for route_no, freq_table in weekdays_freq_tables.items():
        try:

            df = normalize_weekday_freq_table(freq_table, route_no, edge_cases)

            if have_static_departures(df):
                df = df.drop([0], axis=1)

                parsed = pd.DataFrame()

                parsed[0] = df[1].str.split("-").explode().reset_index(drop=True)
                parsed[0] = parsed[0] + "-"
                # Empty columns except first cell with Direction 1
                parsed[1] = pd.Series()
                parsed[2] = df[2].str.split("-").explode().reset_index(drop=True)
                parsed[2] = parsed[2] + "-"
                # parsed[2] = df['your_column'].astype(str)
                # Empty columns except first cell with Direction 2
                parsed[3] = pd.Series()

                parsed.iloc[0, 0] = "Time of departure"
                parsed.iloc[0, 1] = "Direction 1"
                parsed.iloc[0, 2] = "Time of departure"
                parsed.iloc[0, 3] = "Direction 2"

                df = parsed

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

            if df["time_of_departure"].isna().sum() != 0:
                df = enrich_tods(
                    df,
                    parsed_summary_tables.loc[
                        parsed_summary_tables["route_no"] == route_no
                    ][["weekdays_1st_bus", "weekdays_last_bus", "direction"]],
                )

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
            raise Exception(e)

            not_parsable_weekdays_freq_tables[route_no] = freq_table

    return (
        pd.concat(
            [parsed_weekdays_freq_tables[k] for k in parsed_weekdays_freq_tables],
            axis=0,
        ),
        not_parsable_weekdays_freq_tables,
    )


def get_operator(df: pd.DataFrame, x: int, y: int):
    return df.iloc[x, y]


def set_operator(
    df: pd.DataFrame, x: int, y: int, operator_col_name: str
) -> pd.DataFrame:
    operator_name = get_operator(df, x, y)

    df[operator_col_name] = operator_name

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


def process_stage_tables(stage_tbl: pd.DataFrame):
    print(stage_tbl.shape)
