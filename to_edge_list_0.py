import pandas as pd
import argparse


def to_single(df: pd.DataFrame) -> pd.DataFrame:
    def helper(row: pd.Series):
        if row['Team1_Scaled'] > row["Team2_Scaled"]:
            return pd.Series({
                'Date': row['Date'],
                'Team1_Name': row['Team1_Name'],
                'Team2_Name': row['Team2_Name'],
                'Score_Diff': row['Team1_Scaled'] - row['Team2_Scaled'],
                'Map_Name': row['Map_Name']
            })
        else:
            return pd.Series({
                'Date': row['Date'],
                'Team1_Name': row['Team2_Name'],
                'Team2_Name': row['Team1_Name'],
                'Score_Diff': row['Team2_Scaled'] - row['Team1_Scaled'],
                'Map_Name': row['Map_Name']
            })
    other = pd.DataFrame(
        columns=['Date', 'Team1_Name', 'Team2_Name', 'Score_Diff', 'Map_Name'])
    other[['Date', 'Team1_Name', 'Team2_Name', 'Score_Diff',
           'Map_Name']] = df.apply(helper, axis=1)
    return other


def to_double(df: pd.DataFrame) -> pd.DataFrame:
    first = df.drop(columns=['Team2_Scaled'])
    first = first.rename(columns={'Team1_Scaled': 'Scaled_Score'})
    first.index = map(lambda i: 2 * i, first.index.to_list())
    second = df.drop(columns=['Team1_Scaled'])
    second = second.rename(
        columns={'Team1_Name': 'Team2_Name', 'Team2_Name': 'Team1_Name'})
    second = second.rename(columns={'Team2_Scaled': 'Scaled_Score'})
    second.index = map(lambda i: 2 * i + 1, second.index.to_list())
    other = pd.concat([first, second], axis=0).sort_index()
    return other


def main():
    parser = argparse.ArgumentParser(description='provide file path')
    parser.add_argument('--input', required=True,
                        help="csv input file", type=str)
    parser.add_argument('--out_single', required=False,
                        help="csv output file (one edge per match)", type=str)
    parser.add_argument('--out_double', required=False,
                        help="csv output file (two edges per match)", type=str)
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
        to_single(df_rel).to_csv(args.out_single, index=False)
    if not args.out_double is None:
        to_double(df_rel).to_csv(args.out_double, index=False)


main()
