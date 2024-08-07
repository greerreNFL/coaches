## packages ##
import pandas as pd
import numpy
import pathlib
import requests
from bs4 import BeautifulSoup

from .utils import id_from_url

class CoachTable:
    '''
    Table of coaches pulled from pfr. This class handles the reading of
    existing data and appending
    '''
    def __init__(self):
        self.package_loc = pathlib.Path(__file__).parent.parent.resolve()
        self.existing_df = self.load_existing()
        self.scraped_records = []
        self.new_records = []
        self.df = None
        ## save ##
        self.update_and_save()

    def load_existing(self):
        '''
        Attempt to load the local csv if it exists
        '''
        try:
            return pd.read_csv(
                '{0}/coaches/coach_meta.csv'.format(self.package_loc),
                index_col=0
            )
        except:
            return None
    
    def scrape_table(self):
        '''
        Scrapes the PFR coaching table for a list of all ids
        '''
        ## make request ##
        resp = requests.get('https://www.pro-football-reference.com/coaches/')
        ## handle response ##
        if resp.status_code == 200:
            ## if received, parse with bs ##
            soup = BeautifulSoup(resp.content, "html.parser")
            ## find coach cells ##
            coach_tds = soup.findAll('td', {'data-stat' : 'coach'})
            if len(coach_tds) > 0:
                ## if coaches found, scrape each
                ## scrape each coach ##
                for coach in coach_tds:
                    anchor = coach.findAll('a', href=True)
                    if len(anchor) > 0:
                        row = {}
                        row['pfr_coach_id'] = id_from_url(
                            anchor[0]['href']
                        )
                        row['pfr_coach_name'] = anchor[0].text
                        self.scraped_records.append(row)
                    else:
                        ## throw error if scrape failed ##
                        raise Exception('PFR SCRAPE ERROR: Scraped the coaches page, but parser did not find coach anchor IDs')
            else:
                ## throw error if scrape failed ##
                raise Exception('PFR SCRAPE ERROR: Scraped the coaches page, but parser did not find coaches')
        else:
            raise Exception('PFR SCRAPE ERROR: Coaches page failed. {0} - {1}'.format(
                resp.status_code, resp.content
            ))
    
    def merge(self):
        '''
        Merge existing and new records
        '''
        ## init the new table as a df ##
        ## if there are no records, an exception should have been thrown
        ## earlier. Dont check here and instead let the pd.DatFrame file
        ## so the scrape can be investigated and handled
        new = pd.DataFrame(self.scraped_records)
        ## add fields for image urls which are scraped
        new['pfr_coach_image_url'] = numpy.nan
        new['pfr_coach_tree_hired_by'] = numpy.nan
        new['pfr_coach_tree_hired'] = numpy.nan
        new['pfr_coach_last_checked'] = numpy.nan
        ## if no existing, set as existing ##
        if self.existing_df is None:
            self.df = new.copy()
        else:
            self.df = pd.concat([
                self.existing_df,
                new[
                    ~numpy.isin(
                        new['pfr_coach_id'],
                        self.existing_df['pfr_coach_id'].unique().tolist()
                    )
                ].copy()
            ]).reset_index(drop=True)
    
    def update_and_save(self):
        '''
        Scrape, update, and save the coaches table
        '''
        ## scrape the table to populate new records
        self.scrape_table()
        ## update the df ##
        self.merge()
        ## save ##
        self.df.to_csv(
            '{0}/coaches/coach_meta.csv'.format(self.package_loc)
        )
        
                