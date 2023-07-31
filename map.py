from google.cloud import storage
import geopandas as gpd
import pandas as pd
import folium
from glob import glob
import gzip
import re

# init gcs client
gs = storage.Client()
bucket = gs.bucket('dte.lf.lc')

# merge all geojson snapshots into a single frame
outages = []
for blob in gs.list_blobs(bucket.name, prefix='outages'):
    match = re.findall(r'outage-(\d+).geojson', blob.name)
    if not match:
        continue
    ts = int(match[0])
    with blob.open('rb') as f:
        with gzip.open(f) as fz:
            gdf = gpd.read_file(fz, driver='GeoJSON')
    gdf['SNAPSHOT_TTM'] = ts * 1000 # TTM is in nanoseconds
    outages.append(gdf)
merged_outages = gpd.GeoDataFrame(pd.concat(outages, ignore_index=True),
    crs=outages[0].crs)

# group outages by job and calculate the start/end/length times
outage_records = []
for job_id, group in merged_outages.groupby('JOB_ID'):
    start = group['OFF_DTTM'].min() // 1000
    end = group['SNAPSHOT_TTM'].max() // 1000
    if start != start: # sometimes OFF_DTTM is blank, skip these
        continue
    length_of_outage = (end - start) // 3600
    last_cause = group['CAUSE'].iloc[-1]
    geometry = group['geometry'].iloc[-1]

    outage_records.append({
        'job_id': job_id,
        'start': start,
        'end': end,
        'length': length_of_outage,
        'cause': last_cause,
        'geometry': geometry
    })
outage_table = gpd.GeoDataFrame(outage_records, crs=merged_outages.crs)

# group individual jobs into outage events
outage_table_ts = outage_table.copy()
outage_table_ts['start'] = pd.to_datetime(outage_table_ts['start'], unit='s')
outage_table_ts['end'] = pd.to_datetime(outage_table_ts['end'], unit='s')
outage_table_ts = outage_table_ts.sort_values(by='start')

event_id = 0
event_mapping = {}
for index, row in outage_table_ts.iterrows():
    # if the current row started after the current event, make a new event
    if not event_mapping or row['start'] > event_mapping[event_id]['end']:
        event_id += 1
        event_mapping[event_id] = {
            'start': row['start'],
            'end': row['end'],
        }
    else:
        # otherwise, extend the event end time if needed
        event_mapping[event_id]['end'] = max(event_mapping[event_id]['end'], row['end'])
    outage_table.at[index, 'event_id'] = event_id

# export table to gcs
with bucket.blob('outages/merged.geojson').open('wb') as f:
    outage_table.to_file(f, driver='GeoJSON')

# load Ann Arbor data from https://www.a2gov.org/services/data/Pages/default.aspx
landuse = gpd.read_file('data/landuse.geojson')
landuse = landuse.to_crs(outage_table.crs)
landuse['landuse_id'] = range(len(landuse))
landuse = landuse[landuse['GROUP_'] == 'Residential']
landuse_outages = gpd.sjoin(landuse, outage_table,
    how='left', predicate='intersects').groupby('landuse_id')

# calculate average length of outage per land
landuse_avg_length = landuse_outages['length'].mean().reset_index()
landuse_avg_length.rename(columns={'length': 'avg_length'}, inplace=True)
# calculate number of outages per land
landuse_outage_count = landuse_outages['job_id'].nunique().reset_index(name='outage_count')
landuse_outage_count.loc[landuse_outage_count['outage_count'] == 0, 'outage_count'] = None
# merge into single table for plotting
landuse_summary_table = landuse_avg_length.merge(landuse_outage_count, on='landuse_id')
landuse_summary_table = landuse.merge(landuse_summary_table, on='landuse_id')

# plot summary data and export
m = landuse_summary_table.explore(
    column='avg_length',
    name='Average Length',
    tiles='CartoDB dark_matter',
    cmap='plasma',
    style_kwds={'stroke': False},
    missing_kwds={'color': '#00000000'}
)
m = landuse_summary_table.explore(m=m,
    column='outage_count',
    name='Number of Outages',
    tiles='CartoDB dark_matter',
    cmap='plasma',
    style_kwds={'stroke': False},
    missing_kwds={'color': '#00000000'}
)
folium.LayerControl().add_to(m)

# export html to gcs
with bucket.blob('outages/index.html').open('wb', content_type='text/html') as f:
    m.save(f)
