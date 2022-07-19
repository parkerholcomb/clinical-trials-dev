import pandas as pd
import plotly.express as px
from sklearn.cluster import KMeans
from scipy.stats import zscore
from IPython.display import display


def draw_k_means_cluster_map(location: pd.DataFrame, k=15) -> None:
    km = KMeans(
        n_clusters=k,
        init="k-means++",
        n_init=10,
        max_iter=300,
    )
    X = location[["_lat", "_lng"]]
    location["_y_km"] = km.fit_predict(X)
    location["_y_km"] = location["_y_km"].apply(lambda x: f"cluster_{x}")

    px.scatter_mapbox(
        location,
        lat="_lat",
        lon="_lng",
        color="_y_km",
        title=f"Where do Clinical Trials take place? {len(location)} locations | color = _y_km | k={k} clusters",
        size_max=15,
        height=700,
        zoom=3,
    ).show(renderer="notebook")


def draw_org_fragmentation(protocol_features):
    org_counts = (
        protocol_features.groupby(["_org_class", "_org_name"])
        .agg(study_count=("_org_name", "count"))
        .sort_values("study_count", ascending=False)
        .reset_index()
    )
    org_counts["_pct"] = org_counts["study_count"].apply(
        lambda x: x / org_counts["study_count"].sum()
    )
    org_counts["_cumm_pct"] = org_counts["_pct"].cumsum()
    org_counts.to_csv("org_study_counts.csv", index=False)
    display("study count by org",org_counts)
    _study_count = protocol_features["_trial_id"].nunique()
    _org_count = protocol_features["_org_name"].nunique()
    _phases = protocol_features["_phase"].unique()

    def _draw_cumm_dist():
        px.scatter(
            org_counts.reset_index(),
            x="index",
            y="_cumm_pct",
            title=f"Org Cummulative Dist | n={_study_count} studies  | {_phases} <br><sup>Reads: Largest x-axis organzations makes up y-axis percent of total studies",
        ).show(renderer="notebook")

    def _draw_top_treemap(top_n=200):
        cumm_pct = int(org_counts.loc[top_n]['_cumm_pct'] * 100)
        px.treemap(
            org_counts[0:top_n],
            path=["_org_class", "_org_name"],
            values="study_count",
            height=700,
            title=f"Org Fragmentation top {top_n} (of {_org_count} total) account for {cumm_pct}% of studies | grouped by class, scaled by study count | n={_study_count} studies ",
        ).show(renderer="notebook")

    def _draw_org_cumm_dist_by_org_class(org_classes = ["INDUSTRY", "OTHER"]) -> None:
        def _get_cumm_pct_by_class(class_name="INDUSTRY") -> pd.DataFrame:
            df = org_counts[org_counts["_org_class"] == class_name].reset_index(drop=True)
            df["_class_pct"] = df["study_count"].apply(
                lambda x: x / df["study_count"].sum()
            )
            df["_class_cumm_pct"] = df["_class_pct"].cumsum()
            df = df.reset_index().rename(columns={"index": "class_index"})
            return df
        
        df = pd.concat([_get_cumm_pct_by_class(org_class) for org_class in org_classes])
        px.scatter(
            df,
            x="class_index",
            y="_class_cumm_pct",
            color="_org_class",
            title=f"Org Cummulative Dist by Org Class | n={_study_count} studies in {_phases} <br><sup>Reads: Largest x-axis organizations of particular class, makes up y-axis percent of total studies of that class",
        ).show(renderer="notebook")

    _draw_top_treemap()
    _draw_cumm_dist()
    _draw_org_cumm_dist_by_org_class()




