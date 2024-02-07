import requests
import pandas as pd 
import sys
from bs4 import BeautifulSoup
from database import database
import time
from tqdm import tqdm



class scrape:
	def __init__(self, database, start_season, end_season):
		self.database = database
		self.seasons = [str(x-1)+"-"+str(x) for x in range(start_season,end_season+1)]

	def get_leagues(self):
		sql_query = "SELECT * FROM competitions"
		self.leagues = pd.read_sql(sql_query, self.database.conn)

	def create_league_paths(self):
		self.league_paths = []
		for i,l in tqdm(self.leagues[['competition_id', 'competition_name']].values, desc='Creating league paths'):
			l = l.replace(' ', '-')
			if l == 'Europa-Conference-League':
				ecl_season = '2021-2022'
				ind = self.seasons.index(ecl_season)
				print(self.seasons[ind:])
				for s in self.seasons[ind:]:
					path = f'https://fbref.com/en/comps/{i}/{s}/schedule/{s}-{l}-Scores-and-Fixtures'
					self.league_paths.append(path)
			else:
				for s in self.seasons:
					path = f'https://fbref.com/en/comps/{i}/{s}/schedule/{s}-{l}-Scores-and-Fixtures'
					self.league_paths.append(path)

	def open_path(self, path):
		time.sleep(3)
		self.path = path
		self.ses = requests.get(self.path)
		if self.ses.status_code==200:
			self.page_content = BeautifulSoup(self.ses.content, 'html.parser')
		else:
			self.page_content = ''
			print(f'Page not loaded: {self.path}')

	def create_match_paths(self):
		self.match_paths = []
		match_elements = self.page_content.find_all('td', attrs={'data-stat':'match_report'})
		for me in match_elements:
			me_a = me.find('a')
			if me_a!=None:
				if me_a.text=='Match Report':
					url_suff = me_a.get('href')
					self.match_paths.append(f'https://fbref.com{url_suff}')

	def create_all_match_paths(self):
		self.all_match_paths = []
		for path in tqdm(self.league_paths, desc = 'Creating match paths'):
			self.open_path(path)
			self.create_match_paths()
			self.all_match_paths.extend(self.match_paths)
		print('Total number of matches:', len(self.all_match_paths))

if __name__ == '__main__':
	fbref = database(
	        dbname = 'fbref', 
	        user = 'postgres', 
	        port = 5432, 
	        host = 'localhost', 
	        connection_timeout = 10)
	
	start_season = 2018
	end_season = 2024			
	fbref_scrape = scrape(fbref, start_season, end_season)

	fbref_scrape.get_leagues()
	fbref_scrape.create_league_paths()
	fbref_scrape.create_all_match_paths()