from src.ctgov.lib.helpers import trial_keys_query, protocol_query, _get_protocol_feature, DATA_BASE

trial_keys = trial_keys_query()
keys = list(trial_keys['key'])
i = 0
for i in range(0, 50):
    n = 10000
    start = i * n
    end = start + n
    batch_keys = keys[start:end]
    protocol = protocol_query(batch_keys)
    feature = _get_protocol_feature(protocol)
    feature.to_parquet(f"{DATA_BASE}/house/parquet/{n}_{i}.parquet")
    print(i, 'complete')

