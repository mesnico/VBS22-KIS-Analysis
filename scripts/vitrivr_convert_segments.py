import pandas as pd

input_fname = 'data/cineast_segment.json'
output_fname = 'data/v3c1_2_cineast_segments.csv'

df = pd.read_json(input_fname)
df = df.rename(columns={
    'cineast.cineast_segment.objectid': 'video',
    'cineast.cineast_segment.segmentnumber': 'segment',
    'cineast.cineast_segment.segmentstart':'startframe',
    'cineast.cineast_segment.segmentend':'endframe',
    'cineast.cineast_segment.segmentstartabs':'start',
    'cineast.cineast_segment.segmentendabs': 'end'}
)
df['video'] = df['video'].apply(lambda x: x.replace('v_', ''))
df['start'] = df['start'] * 1000    # convert in milliseconds
df['end'] = df['end'] * 1000

df = df.filter(['video', 'segment', 'startframe', 'endframe', 'start', 'end'])
df = df.astype(int)
df.to_csv(output_fname, index=False)