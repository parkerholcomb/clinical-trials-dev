import json
import pandas as pd
from scipy.stats import zscore
from glob import glob
import plotly.express as px
from concurrent.futures import ThreadPoolExecutor

px.set_mapbox_access_token("pk.eyJ1IjoicGFycXVhciIsImEiOiJja3lpcXMycGUxbmF5MnBzZXVzMHBzaXl4In0.jz0tx-HTJWym8jWPa8lqiA")

DATA_BASE = "/Users/parker/Development/clinical-trials-dev/data"


def trial_keys_query() -> pd.DataFrame:
    return pd.DataFrame({"key": glob(f"{DATA_BASE}/lake/AllAPIJSON/*/*.json")})

def protocol_query(keys: list, limit: int) -> pd.DataFrame:
    def _load_study(key) -> dict:
        try:
            with open(key, 'r') as f:
                data = json.load(f)
            return data['FullStudy']['Study']
        except Exception as e:
            print(e)

    if limit:
        keys = trial_keys_query()[0:limit]
    with ThreadPoolExecutor(max_workers=20) as exec:
        futures = exec.map(_load_study, keys)
    study = pd.DataFrame(list(futures))
    protocol = pd.json_normalize(study['ProtocolSection']) # Der
    protocol.columns = [col.split(".")[-1] for col in protocol.columns] # yass, much more readable
    return protocol

def protocol_feature_query() -> pd.DataFrame:
    df = pd.read_parquet(f"{DATA_BASE}/house/parquet")
    
    phases = ['Phase 2','Phase 3','Phase 4']
    df = df[df['_phase'].isin(phases)].reset_index(drop=True)
    return df

def location_query(keys = []) -> pd.DataFrame:
    """
        Get locations of Phase 2, 3, 4
    """
    df = protocol_feature_query()
    df = df[df['_location_count_z'] < 3] # filter < 3 sigma 
    df = df[df['_location_count'] > 0] 
    df = df.sort_values("_location_count")
    df = df[['_trial_id','_location','_phase','_location_count','_condition']].reset_index(drop=True)
    dff = df.explode("_location").reset_index()
    location = pd.json_normalize(dff['_location']) #.reset_index()
    location = location.join(dff[['index','_trial_id','_location_count','_phase','_condition']])
    location = location[location['LocationCountry'] == "United States"]
    location = location.drop(columns=['LocationContactList.LocationContact','LocationCountry','LocationContactList'])
    location = location.dropna(subset=["LocationStatus"]).reset_index(drop=True)

    def _geolocate_zip5(df):
        zip5_census = pd.read_feather("gs://ph-cdn/us_census_zips_geo/adi_stats_zip5.feather")
        zip5_census = zip5_census.drop(columns=['count','adi_mean','min','max','std'])
        return df.merge(zip5_census, left_on="LocationZip", right_on="_zip5")

    location = _geolocate_zip5(location)
    
    return location

def _get_protocol_feature(protocol: pd.DataFrame) -> pd.DataFrame:
    # protocol = protocol_query(limit=limit)
    feature = pd.DataFrame()
    feature_map = {
        "_trial_id":"NCTId",
        '_org_name':'OrgFullName',
        '_org_class':'OrgClass',
        '_sponsor_name':'LeadSponsorName',
        '_sponsor_class':'LeadSponsorClass',
        '_collaborator':'Collaborator',
        # '_type':'StudyType', # They were all intervernational after the dropna below
        '_condition':'Condition',
        '_location':'Location',
        '_eligibility':'EligibilityCriteria',
        '_status':'OverallStatus',
        '_arm': 'ArmGroup',
        '_random': 'DesignAllocation'
    }

    for k, v in feature_map.items():
        feature[k] = protocol[v]

    feature['_enrollment'] = protocol['EnrollmentCount'].dropna().astype(int)
    feature["_phase"] = protocol["Phase"].dropna().apply(lambda x: x[0])
    feature = feature.dropna(subset=['_enrollment','_phase','_random']) # this cleans up a lot
    feature['_location_count'] = protocol['Location'].dropna().apply(lambda x: len(x))
    # feature['_collaborator_count'] = protocol['Collaborator'].dropna().apply(lambda x: len(x))
    # feature['_collaborator_count'] = feature['_collaborator_count'].fillna(0)
    feature['_arm_count'] = protocol['ArmGroup'].dropna().apply(lambda x: len(x))
    feature['_start_yr'] = protocol['StartDate'].apply(lambda x: int(x.split(" ")[-1]) if pd.isna(x) == False else None)
    feature['_end_yr'] = protocol['PrimaryCompletionDate'].apply(lambda x: int(x.split(" ")[-1]) if pd.isna(x) == False else None)
    feature['_last_yr'] = protocol['LastUpdateSubmitDate'].apply(lambda x: int(x.split(" ")[-1]) if pd.isna(x) == False else None)
    # feature['_collaborator_z'] = zscore(feature['_collaborator_count']) 
    feature['_enrollment_z'] = zscore(feature['_enrollment']) 
    feature['_location_count_z'] = zscore(feature['_location_count'].fillna(0)) 
    feature['_arm_count_z'] = zscore(feature['_arm_count'].fillna(0))
    feature = feature.reset_index(drop=True)
    return feature



# def protocol_feature_query(limit: int, force=False) -> pd.DataFrame:
#     if force:
#         protocol = protocol_query(limit=limit)
#         feature = pd.DataFrame()
#         feature_map = {
#             "_trial_id":"NCTId",
#             '_org_name':'OrgFullName',
#             '_org_class':'OrgClass',
#             '_sponsor_name':'LeadSponsorName',
#             '_sponsor_class':'LeadSponsorClass',
#             '_collaborator':'Collaborator',
#             # '_type':'StudyType', # They were all intervernational after the dropna below
#             '_condition':'Condition',
#             '_location':'Location',
#             '_eligibility':'EligibilityCriteria',
#             '_status':'OverallStatus',
#             '_arm': 'ArmGroup',
#             '_random': 'DesignAllocation'
#         }

#         for k, v in feature_map.items():
#             feature[k] = protocol[v]

#         feature['_enrollment'] = protocol['EnrollmentCount'].dropna().astype(int)
#         feature["_phase"] = protocol["Phase"].dropna().apply(lambda x: x[0])
#         feature = feature.dropna(subset=['_enrollment','_phase','_random']) # this cleans up a lot
#         feature['_location_count'] = protocol['Location'].dropna().apply(lambda x: len(x))
#         # feature['_collaborator_count'] = protocol['Collaborator'].dropna().apply(lambda x: len(x))
#         # feature['_collaborator_count'] = feature['_collaborator_count'].fillna(0)
#         feature['_arm_count'] = protocol['ArmGroup'].dropna().apply(lambda x: len(x))
#         feature['_start_yr'] = protocol['StartDate'].apply(lambda x: int(x.split(" ")[-1]) if pd.isna(x) == False else None)
#         feature['_end_yr'] = protocol['PrimaryCompletionDate'].apply(lambda x: int(x.split(" ")[-1]) if pd.isna(x) == False else None)
#         feature['_last_yr'] = protocol['LastUpdateSubmitDate'].apply(lambda x: int(x.split(" ")[-1]) if pd.isna(x) == False else None)
#         # feature['_collaborator_z'] = zscore(feature['_collaborator_count']) 
#         feature['_enrollment_z'] = zscore(feature['_enrollment']) 
#         feature['_location_count_z'] = zscore(feature['_location_count'].fillna(0)) 
#         feature['_arm_count_z'] = zscore(feature['_arm_count'].fillna(0))

#         feature = feature.reset_index(drop=True)
#         feature.to_parquet(f"{DATA_BASE}/house/feauture_{limit}.parquet")
#     else:
#         if limit:
#             try:
#                 feature = pd.read_parquet(f"{DATA_BASE}/house/feauture_{limit}.parquet")
#             except:
#                 input(f"build {limit} cache?")
#                 feature = protocol_feature_query(limit=limit, force=True)
#         else:
#             feature = pd.read_parquet(f"{DATA_BASE}/house/feauture_{limit}.parquet")
        
            
    
#     return feature