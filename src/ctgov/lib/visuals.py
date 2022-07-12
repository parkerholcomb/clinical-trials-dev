import pandas as pd
import plotly.express as px
from sklearn.cluster import KMeans
from scipy.stats import zscore

def draw_k_means_cluster_map(location: pd.DataFrame, k = 15) -> None:
    km = KMeans(
        n_clusters=k, init='k-means++',
        n_init=10, max_iter=300, 
    )
    X = location[["_lat","_lng"]]
    location["_y_km"] = km.fit_predict(X)
    location['_y_km'] = location['_y_km'].apply(lambda x: f"cluster_{x}")

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