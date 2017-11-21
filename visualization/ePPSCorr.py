import pandas as pd

def diff(df):
    df["diff"] = df["expectedPointsAvg"] - df["pointsAvg"]
    df = df[["teamName", "season", "diff", "pointsAvg", "expectedPointsAvg"]]
    return(df)


season = pd.read_csv("data/ePointsPerShotOffenseFullSeason.csv")
thisPoint = pd.read_csv("data/ePointsPerShotOffense4Predict.csv")

seasonFilter = diff(season)
seasonFilter.columns = ["teamName", "season", "season_diff", "season_pointsAvg", "season_expectedPointsAvg"]
thisPointFilter = diff(thisPoint)
thisPointFilter.columns = ["teamName", "season", "thisPoint_diff", "thisPoint_pointsAvg", "thisPoint_expectedPointsAvg"]


new_df = pd.merge(thisPointFilter, seasonFilter,  how='left', left_on=['teamName','season'], right_on = ['teamName','season'])

print(new_df)

new_df.to_csv("data/PPSCorr_Offense.csv")

diffDF = new_df[["teamName", "season", "thisPoint_diff", "season_diff"]]
expectedDF = new_df[["teamName", "season", "thisPoint_expectedPointsAvg", "season_expectedPointsAvg"]]
actualDF = new_df[["teamName", "season", "thisPoint_pointsAvg", "season_pointsAvg"]]


print("diff Corr:")
print(diffDF["thisPoint_diff"].corr(diffDF["season_diff"]))

print("expected Corr:")
print(expectedDF["thisPoint_expectedPointsAvg"].corr(expectedDF["season_expectedPointsAvg"]))

print("actual Corr:")
print(actualDF["thisPoint_pointsAvg"].corr(actualDF["season_pointsAvg"]))