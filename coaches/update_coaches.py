import pandas as pd
import numpy
import pathlib
import json

from .Coach import Coach
from .CoachTable import CoachTable
from .utils import Browser

fp = pathlib.Path(__file__).parent.resolve()

def update_coach_meta():
    '''
    Wrapper to update the coach meta information
    '''
    print('Updating coaching meta data...')
    ## create the coach table ##
    coach_table = CoachTable()
    ## update ##
    records = []
    for record in coach_table.df.to_dict('records'):
        ## for each record, create the update as requried ##
        try:
            ## init a coach, which handles SLA, update, etc ##
            coach = Coach(record)
            ## append the handled record ##
            records.append(coach.record)
        except Exception as e:
            print('     Coach instance could not be created')
            print('          {0}'.format(e))
            records.append(record)
    ## cleanup browser when done ##
    Browser().stop()
    ## combine ##
    df = pd.DataFrame(records)
    ## apply hs overrides ##
    with open('{0}/img_overrides.json'.format(fp)) as f:
        img_map = json.load(f)
    df['pfr_coach_image_url'] = df['pfr_coach_id'].map(img_map).combine_first(df['pfr_coach_image_url'])
    ## save ##
    df.to_csv(
        '{0}/coach_meta.csv'.format(fp)
    )
