import pandas as pd
import argparse
import os


def filter_map(df: pd.DataFrame, map_name: str) -> pd.DataFrame:
    if map_name is None:
        return df
    return df[df['Map_Name'].str.contains(map_name)]


def filter_team(df: pd.DataFrame, team_name: str) -> pd.DataFrame:
    if team_name is None:
        return df
    return df[(df['Target'] == team_name) | (df['Source'] == team_name)]


def to_single(df: pd.DataFrame, map_name: str = None, team: str = None) -> pd.DataFrame:
    def helper(row: pd.Series):
        if row['Team1_Scaled'] > row["Team2_Scaled"]:
            return pd.Series({
                'Date': row['Date'],
                'Source': row['Team1_Name'],
                'Target': row['Team2_Name'],
                'Score_Diff': row['Team1_Scaled'] - row['Team2_Scaled'],
                'Map_Name': row['Map_Name']
            })
        else:
            return pd.Series({
                'Date': row['Date'],
                'Source': row['Team2_Name'],
                'Target': row['Team1_Name'],
                'Score_Diff': row['Team2_Scaled'] - row['Team1_Scaled'],
                'Map_Name': row['Map_Name']
            })
    other = pd.DataFrame(
        columns=['Date', 'Source', 'Target', 'Score_Diff', 'Map_Name'])
    other[['Date', 'Source', 'Target', 'Score_Diff',
           'Map_Name']] = df.apply(helper, axis=1)
    return filter_map(filter_team(other, team), map_name)


def to_double(df: pd.DataFrame, map_name: str = None, team: str = None) -> pd.DataFrame:
    first = df.drop(columns=['Team2_Scaled'])
    first = first.rename(columns={
                         'Team1_Scaled': 'Scaled_Score', 'Team1_Name': 'Source', 'Team2_Name': 'Target'})
    first.index = map(lambda i: 2 * i, first.index.to_list())
    second = df.drop(columns=['Team1_Scaled'])
    second = second.rename(
        columns={'Team1_Name': 'Team2_Name', 'Team2_Name': 'Team1_Name'})
    second = second.rename(columns={'Team2_Scaled': 'Scaled_Score'})
    second = second.rename(
        columns={'Team1_Name': 'Source', 'Team2_Name': 'Target'})
    second.index = map(lambda i: 2 * i + 1, second.index.to_list())
    other = pd.concat([first, second], axis=0).sort_index()
    return filter_map(filter_team(other, team), map_name)


def split_by_date_intervals(df: pd.DataFrame, dates: list[str]) -> list[pd.DataFrame]:
    start_date = pd.to_datetime(df['Date'].min())
    end_date = pd.to_datetime(df['Date'].max())
    d: list[pd.Timestamp] = list(map(pd.to_datetime, dates))
    d = filter(lambda t: t >= start_date and t <= end_date, d)
    d = list(sorted(d))
    _dates = []
    if start_date <= d[0]:
        _dates.append(start_date)
    _dates.extend(d)
    if end_date >= d[len(d) - 1]:
        _dates.append(end_date)
    return [df[df['Date'].between(_dates[i], _dates[i + 1])] for i in range(len(_dates) - 1)]


def save_dataframes(dfs: list[pd.DataFrame], prefix: str, target_dir: str) -> None:
    os.mkdir(target_dir)
    for df in dfs:
        file = os.path.join(target_dir, f"{prefix}_{df['Date'].max()}")
        df.to_csv(file, index=False)


def main():
    parser = argparse.ArgumentParser(description='provide file path')
    parser.add_argument('--input', required=True,
                        help="csv input file", type=str)
    parser.add_argument('--out_single', required=False,
                        help="csv output file (one edge per match)", type=str)
    parser.add_argument('--out_double', required=False,
                        help="csv output file (two edges per match)", type=str)
    parser.add_argument('--dates', required=False, nargs='+',
                        help="intervals", type=str)
    parser.add_argument('--team', required=False,
                        help="filter team", type=str)
    parser.add_argument('--map', required=False,
                        help="filter map", type=str)
    args = parser.parse_args()

    df = pd.read_csv(args.input, sep=',')
    df['Date'] = pd.to_datetime(df['Date'])
    df = df[["Date", "Team1_Name", "Team1_Score",
             "Team2_Name", "Team2_Score", "Map_Name"]]
    df_rel = df.copy()
    df_rel["Team1_Scaled"] = (
        df_rel["Team1_Score"] / df_rel[["Team1_Score", "Team2_Score"]].max(axis=1))
    df_rel["Team2_Scaled"] = (
        df_rel["Team2_Score"] / df_rel[["Team1_Score", "Team2_Score"]].max(axis=1))
    df_rel = df_rel.drop(columns=["Team1_Score", "Team2_Score"])
    if not args.out_single is None:
        if not args.dates is None:
            save_dataframes(split_by_date_intervals(
                to_single(df_rel, map_name=args.map, team=args.team), args.dates), "single", args.out_single)
        else:
            to_single(df_rel, map_name=args.map, team=args.team).to_csv(
                args.out_single, index=False)
    if not args.out_double is None:
        if not args.dates is None:
            save_dataframes(split_by_date_intervals(
                to_double(df_rel, map_name=args.map, team=args.team), args.dates), "double", args.out_double)
        else:
            to_double(df_rel, map_name=args.map, team=args.team).to_csv(
                args.out_double, index=False)


main()
