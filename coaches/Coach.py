import pandas as pd
import numpy
import datetime
import pathlib
import json
import time
import random
import requests
from bs4 import BeautifulSoup

from .utils import id_from_url, Browser

class Coach:
    '''
    Takes a row record from the coaches table and handles
    the SLA, scraping, and saving of profile URLs
    '''
    def __init__(self, record, sla_days=365):
        self.id = record['pfr_coach_id']
        self.record = record
        self.sla_days = sla_days
        self.last_update = self.determine_last_fetch()
        self.current_date = datetime.datetime.today().strftime('%Y-%m-%d')
        self.fetch_required = self.determine_fetch_requirement()
        ## update data if required
        if self.fetch_required:
            print('     Updating {0}'.format(record['pfr_coach_name']))
            self.fetch_data()
        ## after instance is init, record can be referenced

    def determine_last_fetch(self):
        '''
        Checks record for a last updated date
        '''
        try:
            return datetime.datetime.strptime(
                self.record['pfr_coach_last_checked'],
                '%Y-%m-%d'
            ).date()
        except:
            return None
        
    def determine_fetch_requirement(self):
        '''
        check the SLA on the coach meta data
        '''
        ## if no update can be found, fetch is required
        if self.last_update is None:
            return True
        ## otherwise, if last fetch is outside of the sla
        ## a fetch is required
        if (
            datetime.datetime.strptime(
                self.current_date,
                '%Y-%m-%d'
            ).date() -
            self.last_update
        ).days > 365:
            return True
        ## if no conditions are met, do not update ##
        return False

    def employment_table_helper(self, parsed_bs4, key):
        '''
        A helper to handle pfr's commented out tables
        '''
        ## return struc ##
        return_array = []
        try:
            ## get the commented table ##
            section = str(parsed_bs4.find('div', {'id' : 'all_{0}'.format(key)}))
            new_parsed = BeautifulSoup(section.split('<!--')[1].split('-->')[0], 'html.parser')
            ## now parse normally ##
            table = new_parsed.findAll(
                'table', {'id' : key}
            )
            if len(table) > 0:
                coaches = table[0].findAll(
                    'th', {'data-stat' : 'coach_name'}
                )
                if len(coaches) > 0:
                    for c in coaches:
                        anchor = c.findAll('a', href=True)
                        if len(anchor) > 0:
                            coach_id = id_from_url(
                                anchor[0]['href']
                            )
                            if coach_id not in return_array:
                                return_array.append(coach_id)
        except Exception as e:
            pass
        ## return ##
        return return_array
        

    def scrape_coach(self):
        '''
        Scrapes the coaches profile if an update is required
        '''
        ## scrape pfr coaching page ##
        time.sleep(5 + random.random() * 5)
        try:
            ## use singleton browser instance ##
            browser = Browser()
            page_html = browser.get_page_html(
                'https://www.pro-football-reference.com/coaches/{0}.htm'.format(
                    self.id
                )
            )
        except Exception as e:
            raise Exception('PFR COACH SCRAPE ERROR: Could not scrape {0}: {1}'.format(
                self.id, e
            ))
        ## struc ##
        img_url = numpy.nan
        hired_by_array = []
        hired_array = []
        ## parse ##
        soup = BeautifulSoup(page_html, "html.parser")
        ## find image cells ##
        image_block = soup.findAll('div', {'id' : 'meta'})
        if len(image_block) > 0:
            img = image_block[0].findAll('img')
            if len(img) > 0:
                img_url = img[0]['src']
        else:
            pass
        ## find coaching tree ##
        ## worked for ##
        hired_by_array = self.employment_table_helper(soup, 'worked_for')
        hired_array = self.employment_table_helper(soup, 'employed')
        ## return results ##
        return (
            img_url,
            ','.join(hired_by_array) if len(hired_by_array) > 0 else numpy.nan,
            ','.join(hired_array) if len(hired_array) > 0 else numpy.nan
        )
    
    def fetch_data(self):
        '''
        Scrapes the coach and joins to the record
        '''
        try:
            img_url, hired_by, hired = self.scrape_coach()
        except Exception as e:
            print('Could not scrape {0}:'.format(self.id))
            print(e)
            img_url=numpy.nan
            hired_by=numpy.nan
            hired=numpy.nan
        ## update record ##
        self.record['pfr_coach_image_url'] = self.record['pfr_coach_image_url'] if pd.isnull(img_url) else img_url
        self.record['pfr_coach_tree_hired_by'] = self.record['pfr_coach_tree_hired_by'] if pd.isnull(hired_by) else hired_by
        self.record['pfr_coach_tree_hired'] = self.record['pfr_coach_tree_hired'] if pd.isnull(hired) else hired
        self.record['pfr_coach_last_checked'] = self.current_date