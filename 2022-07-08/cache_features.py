import json
import pandas as pd
from scipy.stats import zscore
from glob import glob
import plotly.express as px

def _load_study(key) -> dict:
    with open(key, 'r') as f:
        data = json.load(f)
    return data['FullStudy']['Study']

# now we have a batch of studies in memory, which we could write to parquet? or just keep on hand? Idk we'll figure it out

def protocol_feature_query(limit=1000) -> pd.DataFrame:
    """
        Opens up studies, normalizes to DataFrame 
    """
    DATA_BASE = "/Users/parker/Development/clinical-trials-dev/data"
    keys = glob(f"{DATA_BASE}/AllAPIJSON/*/*.json") # download and unpack from ClinicalTrials.gov
    study = pd.DataFrame([_load_study(key) for key in keys[0:limit]])
    protocol = pd.json_normalize(study['ProtocolSection']) # Der
    protocol.columns = [col.split(".")[-1] for col in protocol.columns] # yass
    feature = pd.DataFrame()
    feature_map = {
        # "_id":"NCTId",
        '_org_name':'OrgFullName',
        # '_org_class':'OrgClass',
        '_sponsor_name':'LeadSponsorName',
        # '_sponsor_class':'LeadSponsorClass',
        # '_collaborator':'Collaborator',
        # '_type':'StudyType', # They were all intervernational after the dropna below
        # '_condition':'Condition',
        '_location':'Location',
        # '_eligibility':'EligibilityCriteria',
        '_enrollment': "EnrollmentCount",
        '_status':'OverallStatus',
        '_phase':'Phase',
        '_arm': 'ArmGroup',
        '_random': 'DesignAllocation'
    }

    for k, v in feature_map.items():
        feature[k] = protocol[v]

    feature = feature.dropna(subset=['_enrollment','_phase','_random']) # this cleans up a lot
    
    feature['_location_count'] = protocol['Location'].dropna().apply(lambda x: len(x))
    feature['_arm_count'] = protocol['ArmGroup'].dropna().apply(lambda x: len(x))
    feature['_start_yr'] = protocol['StartDate'].apply(lambda x: int(x.split(" ")[-1]) if pd.isna(x) == False else None)
    feature['_end_yr'] = protocol['PrimaryCompletionDate'].apply(lambda x: int(x.split(" ")[-1]) if pd.isna(x) == False else None)
    feature['_last_yr'] = protocol['LastUpdateSubmitDate'].apply(lambda x: int(x.split(" ")[-1]) if pd.isna(x) == False else None)
    
    feature["_phase"] = feature["_phase"].dropna().apply(lambda x: x[0])
    feature['_enrollment_z'] = zscore(feature['_enrollment'].astype(int)) # whats up here?
    feature['_arm_count_z'] = zscore(feature['_arm_count'].fillna(0))
    
    return feature

protocol_feature = protocol_feature_query(limit=1000)
protocol_feature.to_feather(f"cache_{protocol_feature}.feather")