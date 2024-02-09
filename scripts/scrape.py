import requests
import pandas as pd 
import sys
from bs4 import BeautifulSoup
from database import database
import time
from tqdm import tqdm
import os 
from datetime import datetime

class scrape:
    def __init__(self, start_season, end_season, competitions = 'all'):

        all_competitions = [['Premier League',9],
        ['La Liga',12],
        ['Ligue 1',13],
        ['Bundesliga',20],
        ['Seria A',11],
        ['Primeira Liga',32],
        ['Eredivisie',23],
        ['Championship',10],
        ['Segunda Division',17],
        ['2 Bundesliga',33],
        ['Seria B',18],
        ['Champions League',8],
        ['Europa League',19],
        ['Europa Conference League',882]]

        self.all_competitions = pd.DataFrame(all_competitions, columns = ['competition_name', 'competition_id'])
        self.competitions = competitions 

        if self.competitions == 'all':

            self.selected_competitions = self.all_competitions
            print('All competitions selected:')
            print(self.all_competitions)

        elif self.competitions == 'partial':

            print(self.all_competitions)
            competition_ids = input('Insert competition ids. Separate it with comma ->   ').split(',')
            competition_ids = [int(x) for x in competition_ids]
            self.selected_competitions = self.all_competitions.loc[self.all_competitions.competition_id.isin(competition_ids)]

        else:
            print("Please insert 'all' or 'partial' for competittions argument!")
            sys.exit()

        self.seasons = [str(x-1)+"-"+str(x) for x in range(start_season,end_season+1)]
        self.selected_competitions = self.selected_competitions.copy()
        self.selected_competitions['competition_name'] = self.selected_competitions['competition_name'].str.replace(' ', '-')


    def generate_competition_paths(self):

        url = 'https://fbref.com/en/comps/{comp_id}/{s}/schedule/{s}-{comp}-Scores-and-Fixtures'
        self.comp_paths = [url.format(comp_id=comp_id, s = s, comp = comp) for s in tqdm(self.seasons, desc='Generating competition paths') for comp_id,comp in self.selected_competitions[['competition_id', 'competition_name']].values]


        return self.comp_paths

    def open_page(self, path):
        time.sleep(3)
        path = path
        ses = requests.get(path)
        if ses.status_code==200:
            self.page_content = BeautifulSoup(ses.content, 'html.parser')
            return self.page_content
        else:
            return None

    def generate_match_paths(self):

        core_url = 'https://fbref.com'

        page_contents = [self.open_page(comp) for comp in tqdm(self.comp_paths, desc='Generating match paths')]
        match_elements = [pc.find_all('td', attrs={'data-stat':'match_report'}) for pc in page_contents if pc!=None]
        me_a = [x.find('a') for me in match_elements for x in me]
        self.match_paths = [core_url+x.get('href') for x in me_a if x!=None and x.text=='Match Report']


        return self.match_paths

    def generate_html(self, path, store = True):

        self.store = store
        self.page_content = self.open_page(path)
        if self.store:
            directory = 'data/html/'
            if not os.path.exists(directory):
                os.makedirs(directory)
            file_path = os.path.join(directory, path.split('/')[-1])
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(self.page_content.prettify())
        else:
            pass
        return self.page_content

    def scrape_match_info(self, mpath, html):

        self.mp = mp
        self.match_id = self.mp.split('/')[-2]
        self.html = html

        self.scorebox = html.find('div', {'class':'scorebox'})
        self.scorebox_meta = self.scorebox.find('div', {'class':'scorebox_meta'})
        self.get_competition_id()
        self.get_team_info()
        self.get_managers()
        self.get_captains()
        self.get_datetime()
        self.get_gameweek()
        self.get_attendance()
        self.get_stadium_details()
        self.get_referee()

        data = [self.cid, self.match_id, self.gw, self.day, self.date, self.timee, self. stadium, self.city, self.attendance, self.referee]
        print(data)

        return data

    def get_competition_id(self):

        self.cid = self.scorebox_meta.find_all('div')
        self.cid = [x.find('a').get('href').split('/')[-2] for x in self.cid if 'Matchweek' in x.text][0]

    def get_team_info(self):

        teams = self.scorebox.find_all('img', {'class':'teamlogo'})
        self.teams = [x.get('alt').split(' Club Crest')[0] for x in teams]
        self.team_ids = [x.get('src').split('/')[-1].split('.')[0] for x in teams]
        self.ground = ['H', 'A']

    def get_managers(self):

        managers = self.scorebox.find_all(string = 'Manager')
        self.managers = [ x.find_parent().find_parent().text.split(": ")[-1].replace('\xa0', ' ') for x in managers]

    def get_captains(self):

        captains = self.scorebox.find_all(string = 'Captain')
        self.captains = [ x.find_parent().find_parent().text.split(": ")[-1].replace('\xa0', ' ') for x in captains]
        self.captain_ids = [ x.find_parent().find_parent().find('a').get('href').split("/")[-2] for x in captains]

    def get_datetime(self):
        
        self.datetime = self.scorebox_meta.find('span', {'class':'venuetime'})
        date = self.datetime.get('data-venue-date')
        timee = self.datetime.get('data-venue-time')
        date = datetime.strptime(date, '%Y-%m-%d').date()
        timee = datetime.strptime(timee, '%H:%M').time()
        self.date = date.isoformat()
        self.timee = timee.strftime('%H:%M:%S')
        self.day = self.datetime.find_previous_sibling().text.split(" ")[0]

    def get_gameweek(self):

        self.gw = self.scorebox_meta.find_all('div')
        self.gw = [x.text.split(" ")[-1].split(')')[0] for x in self.gw if 'Matchweek' in x.text]
        self.gw = self.gw[0] if len(self.gw)>0 else None

    def get_attendance(self):

        self.attendance = int(self.scorebox_meta.find(string='Attendance').find_parent().find_parent().find_parent().text.split(":")[-1].replace(',',''))

    def get_stadium_details(self):

        venue = self.scorebox_meta.find(string='Venue').find_parent().find_parent().find_parent().text
        self.stadium = venue.split(':')[-1].split(',')[0].strip()
        self.city = venue.split(",")[-1].strip()

    def get_referee(self):

        self.referee = self.scorebox_meta.find(string='Officials').find_parent().find_parent().find_parent().text.split(":")[-1].split("(")[0].strip().replace('\xa0', ' ')

if __name__ == '__main__':

    fb_scrape = scrape(competitions = 'partial',
                        start_season = 2024,
                        end_season = 2024)

    comp_paths = fb_scrape.generate_competition_paths()
    match_paths = fb_scrape.generate_match_paths()

    for mp in tqdm(match_paths):
        html = fb_scrape.generate_html(mp, store = True)
        fb_scrape.scrape_match_info(mp, html)