## packages ##
import pandas as pd
import numpy
import codecs
import pathlib
import json

import nfelodcm as dcm

class StatCompiler:
    '''
    Compiles coaching stats and adds to the coaching meta
    '''

    def __init__(self):
        ## meta ##
        self.package_loc = pathlib.Path(__file__).parent.parent.resolve()
        ## datasets ##
        self.games, self.logos = self.fetch_external()
        self.coach_meta = pd.read_csv(
            '{0}/coaches/coach_meta.csv'.format(self.package_loc),
            index_col=0
        )
        self.pre_fastr_deltas = pd.read_csv(
            '{0}/stats/pre_99_coaching_deltas.csv'.format(self.package_loc),
            index_col=0
        )
        ## final output ##
        self.compiled_stats = self.aggregate_games()
        ## enrich ##
        self.add_teams()
        self.add_coach_meta()
        ## save ##
        self.save_output()

    def fetch_external(self):
        '''
        Gets external dataset using nfelodcm
        '''
        db = dcm.load(['games', 'logos'])
        db['games'] = db['games'][
            ~pd.isnull(db['games']['result'])
        ].copy()
        return db['games'], db['logos']
    
    def add_deltas_to_games(self, df, deltas):
        '''
        Adds data from before fastR to the dataset to make sure it is accurate
        for all time numbers like wins and losses, etc. This will not handle ATS
        information
        '''
        ## make sure whitespace is removed ##
        deltas['coach'] = deltas['coach'].str.strip()
        ## if coach is in DF, then add delta from fields ##
        ## if coach is not in df then add to new record ##
        existing = []
        ## missing ##
        missing = []
        for index, row in deltas.iterrows():
            coach = row['coach']
            existing_rec = df[df['coach'] == coach]
            if len(existing_rec) == 0:
                missing.append({
                    'coach': coach,
                    'seasons' : row['seasons'],
                    'is_active' : 0,
                    'games' : row['games'],
                    'wins' : row['wins'],
                    'losses' : row['losses'],
                    'ties' : row['ties'],
                    'playoff_births' : row['playoff_births'],
                    'games_playoff' : row['games_playoff'],
                    'wins_playoff' : row['wins_playoff'],
                    'losses_playoff' : row['losses_playoff'],
                    'ties_playoff' : 0,
                    'games_superbowl' : row['games_superbowl'],
                    'wins_superbowl' : row['wins_superbowl'],
                    'ats_pct' : numpy.nan,
                    'ats_return' : numpy.nan,
                    'ats_risked' : numpy.nan,
                    'avg_pf' : numpy.nan,
                    'avg_pa' : numpy.nan,
                    'avg_margin' : numpy.nan,
                    'avg_spread' : numpy.nan,
                    'ats_pct_home' : numpy.nan,
                    'ats_pct_away' : numpy.nan,
                    'ats_pct_playoff' : numpy.nan,
                    'ats_pct_favorite' : numpy.nan,
                    'ats_pct_underdog' : numpy.nan,
                    'ats_pct_div' : numpy.nan,
                    'ats_pct_non_div' : numpy.nan,
                    'ats_pct_bye' : numpy.nan,
                    'ats_pct_dome' : numpy.nan,
                })
            else:
                record = existing_rec.iloc[0]
                existing.append({
                    'coach': record['coach'],
                    'seasons' : row['seasons'] + record['seasons'],
                    'is_active' : record['is_active'],
                    'games' : row['games'] + record['games'],
                    'wins' : row['wins'] + record['wins'],
                    'losses' : row['losses'] + record['losses'],
                    'ties' : row['ties'] + record['ties'],
                    'playoff_births' : row['playoff_births'] + record['playoff_births'],
                    'games_playoff' : row['games_playoff'] + record['games_playoff'],
                    'wins_playoff' : row['wins_playoff'] + record['wins_playoff'],
                    'losses_playoff' : row['losses_playoff'] + record['losses_playoff'],
                    'ties_playoff' : record['ties_playoff'],
                    'games_superbowl' : row['games_superbowl'] + record['games_superbowl'],
                    'wins_superbowl' : row['wins_superbowl'] + record['wins_superbowl'],
                    'ats_pct' : record['ats_pct'],
                    'ats_return' : record['ats_return'],
                    'ats_risked' : record['ats_risked'],
                    'avg_pf' : record['avg_pf'],
                    'avg_pa' : record['avg_pa'],
                    'avg_margin' : record['avg_margin'],
                    'avg_spread' : record['avg_spread'],
                    'ats_pct_home' : record['ats_pct_home'],
                    'ats_pct_away' : record['ats_pct_away'],
                    'ats_pct_playoff' : record['ats_pct_playoff'],
                    'ats_pct_favorite' : record['ats_pct_favorite'],
                    'ats_pct_underdog' : record['ats_pct_underdog'],
                    'ats_pct_div' : record['ats_pct_div'],
                    'ats_pct_non_div' : record['ats_pct_non_div'],
                    'ats_pct_bye' : record['ats_pct_bye'],
                    'ats_pct_dome' : record['ats_pct_dome'],
                })
        ## turn into dfs and merge ##
        existing_df = pd.DataFrame(existing)
        missing_df = pd.DataFrame(missing)
        new_df = pd.concat([existing_df, missing_df])
        ## add exisint coaches that arent in new ##
        not_in_new = df[~df['coach'].isin(new_df['coach'])]
        if len(not_in_new) > 0:
            new_df = pd.concat([new_df, not_in_new])
        ## resort ##
        new_df = new_df.sort_values(
            by=['wins'],
            ascending=[False]
        ).reset_index(drop=True)
        new_df = new_df.groupby(['coach']).head(1)
        return new_df

    def aggregate_games(self):
        '''
        Aggregates the games file into coaching records
        '''
        ## copy games for convenience of not writing self.games repeatedly
        df = self.games.copy()
        ## add game context
        df['home_result'] = df['result']
        df['away_result'] = -1 * df['home_result']
        df['home_spread'] = df['spread_line'] * -1
        df['away_spread'] = df['spread_line']
        df['playoffs'] = numpy.where(
            df['game_type'] != 'REG',
            1,
            0
        )
        df['home_is_home'] = 1
        df['away_is_home'] = 0
        df['last_week'] = df.groupby(['season'])['week'].transform('max')
        df['superbowl'] = numpy.where(
            df['week'] == df['last_week'],
            1,
            0
        )
        ## add byes ##
        df['home_bye'] = numpy.where(
            df['home_rest'] > 11,
            1,
            0
        )
        df['away_bye'] = numpy.where(
            df['away_rest'] > 11,
            1,
            0
        )
        df['in_dome'] = numpy.where(
            numpy.isin(
                df['roof'],
                ['dome', 'closed'],
            ),
            1,
            0
        )
        ## flatten games
        flat = pd.concat([
            df[[
                'season', 'home_coach', 'home_team', 'week',
                'home_score', 'away_score',
                'home_spread',
                'home_result',
                'home_is_home',
                'playoffs', 'superbowl',
                'home_bye', 'in_dome', 'div_game'
            ]].rename(columns={
                'home_coach': 'coach',
                'home_team': 'team',
                'home_score': 'pf',
                'away_score': 'pa',
                'home_result': 'result',
                'home_spread': 'spread',
                'home_is_home': 'is_home',
                'home_bye': 'bye',
            }),
            df[[
                'season', 'away_coach', 'away_team', 'week',
                'away_score', 'home_score',
                'away_spread',
                'away_result',
                'away_is_home',
                'playoffs', 'superbowl',
                'away_bye', 'in_dome', 'div_game'
            ]].rename(columns={
                'away_coach': 'coach',
                'away_team': 'team',
                'away_score': 'pf',
                'home_score': 'pa',
                'away_result': 'result',
                'away_spread': 'spread',
                'away_is_home': 'is_home',
                'away_bye': 'bye',
            })
            ])
        ## create fields to aggregate ##
        flat['win'] = numpy.where(
            flat['result'] > 0,
            1,
            0
        )
        flat['loss'] = numpy.where(
            flat['result'] < 0,
            1,
            0
        )
        flat['tie'] = numpy.where(
            flat['result'] == 0,
            1,
            0
        )
        flat['ats_result'] = numpy.where(
            flat['result'] + flat['spread'] > 0,
            1,
            numpy.where(
                flat['result'] + flat['spread'] < 0,
                0,
                numpy.nan
            )
        )
        flat['ats_return'] = numpy.where(
            flat['ats_result'] == 1,
            1,
            numpy.where(
                flat['ats_result'] == 0,
                -1.1,
                0
            )
        )
        flat['ats_risked'] = 1.1
        flat['win_playoff'] = numpy.where(
            flat['playoffs'] == 1,
            numpy.where(
                flat['result'] > 0,
                1,
                0
            ),
            numpy.nan
        )
        flat['loss_playoff'] = numpy.where(
            flat['playoffs'] == 1,
            numpy.where(
                flat['result'] < 0,
                1,
                0
            ),
            numpy.nan
        )
        flat['tie_playoff'] = numpy.where(
            flat['playoffs'] == 1,
            numpy.where(
                flat['result'] == 0,
                1,
                0
            ),
            numpy.nan
        )
        flat['playoff_season'] = numpy.where(
            flat['playoffs'] == 1,
            flat['season'],
            numpy.nan
        )
        flat['win_superbowl'] = numpy.where(
            flat['superbowl'] == 1,
            numpy.where(
                flat['result'] > 0,
                1,
                0
            ),
            numpy.nan
        )
        ## ATS Splits ##
        flat['ats_home'] = numpy.where(
            flat['is_home'] == 1,
            flat['ats_result'],
            numpy.nan
        )
        flat['ats_away'] = numpy.where(
            flat['is_home'] == 0,
            flat['ats_result'],
            numpy.nan
        )
        flat['ats_playoff'] = numpy.where(
            flat['playoffs'] == 1,
            flat['ats_result'],
            numpy.nan
        )
        flat['ats_favorite'] = numpy.where(
            flat['spread'] < 0,
            flat['ats_result'],
            numpy.nan
        )
        flat['ats_underdog'] = numpy.where(
            flat['spread'] > 0,
            flat['ats_result'],
            numpy.nan
        )
        flat['ats_div'] = numpy.where(
            flat['div_game'] == 1,
            flat['ats_result'],
            numpy.nan
        )
        flat['ats_non_div'] = numpy.where(
            flat['div_game'] == 0,
            flat['ats_result'],
            numpy.nan
        )
        flat['ats_bye'] = numpy.where(
            flat['bye'] == 1,
            flat['ats_result'],
            numpy.nan
        )
        flat['ats_dome'] = numpy.where(
            flat['in_dome'] == 1,
            flat['ats_result'],
            numpy.nan
        )
        ## get active coaches ##
        active = flat.sort_values(
            by=['team','season','week'],
            ascending=[True,True,True]
        ).groupby(['team']).tail(1)[
            'coach'
        ].unique().tolist()
        flat['is_active'] = numpy.where(
            numpy.isin(flat['coach'], active),
            1,
            0
        )
        ## aggregate ##
        agg = flat.groupby(['coach']).agg(
            seasons = ('season', 'nunique'),
            is_active = ('is_active', 'max'),
            games = ('season', 'count'),
            wins = ('win', 'sum'),
            losses = ('loss', 'sum'),
            ties = ('tie', 'sum'),
            playoff_births = ('playoff_season', 'nunique'),
            games_playoff = ('playoffs', 'sum'),
            wins_playoff = ('win_playoff', 'sum'),
            losses_playoff = ('loss_playoff', 'sum'),
            ties_playoff = ('tie_playoff', 'sum'),
            games_superbowl = ('superbowl', 'sum'),
            wins_superbowl = ('win_superbowl', 'sum'),
            ats_pct = ('ats_result', 'mean'),
            ats_return = ('ats_return', 'sum'),
            ats_risked = ('ats_risked', 'sum'),
            avg_pf = ('pf', 'mean'),
            avg_pa = ('pa', 'mean'),
            avg_margin = ('result', 'mean'),
            avg_spread = ('spread', 'mean'),
            ats_pct_home = ('ats_home', 'mean'),
            ats_pct_away = ('ats_away', 'mean'),
            ats_pct_playoff = ('ats_playoff', 'mean'),
            ats_pct_favorite = ('ats_favorite', 'mean'),
            ats_pct_underdog = ('ats_underdog', 'mean'),
            ats_pct_div = ('ats_div', 'mean'),
            ats_pct_non_div = ('ats_non_div', 'mean'),
            ats_pct_bye = ('ats_bye', 'mean'),
            ats_pct_dome = ('ats_dome', 'mean')
        ).reset_index()
        agg = agg.sort_values(
            by=['wins'],
            ascending=[False]
        ).reset_index(drop=True)
        ## add deltas from before 1999 ##
        agg = self.add_deltas_to_games(agg, self.pre_fastr_deltas)
        ## calc some post agg fields ##
        agg['ats_roi'] = agg['ats_return'] / agg['ats_risked']
        agg['win_pct'] = agg['wins'] / (agg['wins']+agg['losses']+agg['ties'])
        agg['win_pct_playoff'] = agg['wins_playoff'] / (agg['wins_playoff']+agg['losses_playoff']+agg['ties_playoff'])
        ## return ##
        return agg

    def add_teams(self):
        '''
        Adds an array of teams that the coach coached for
        '''
        ## load games #
        g = self.games.copy()
        ## flatten ##
        g = g[~pd.isnull(g['result'])]
        ## flatten ##
        flat = pd.concat([
            g[[
                'home_coach', 'home_team',
            ]].rename(columns={
                'home_coach': 'coach',
                'home_team': 'team'
            }),
            g[[
                'away_coach', 'away_team',
            ]].rename(columns={
                'away_coach': 'coach',
                'away_team': 'team'
            })
        ])
        ## aggregate by games coached by team to get a good order ##
        agg = flat.groupby(['coach', 'team']).agg(
            games = ('team', 'count')
        ).reset_index()
        agg = agg.sort_values(
            by=['coach', 'games'],
            ascending=[True, False]
        ).reset_index(drop=True)
        ## add colors and abr ##
        logo = self.logos.copy()
        agg = pd.merge(
            agg,
            logo[[
                'team_abbr', 'team_color'
            ]].rename(columns={
                'team_abbr' : 'team'
            }),
            on=['team'],
            how='left'
        )
        ## make a field that can be turned into json of team and color ##
        agg['team_json'] = '{"team" : "' + agg['team'] + '", "color" : "' + agg['team_color'] + '"}'
        ## aggregate by coach ##
        agg = agg.groupby(['coach']).agg(
            teams = ('team_json', lambda x: x.tolist())
        ).reset_index()
        ## add to df ##
        self.compiled_stats = pd.merge(
            self.compiled_stats,
            agg,
            on=['coach'],
            how='left'
        )

    def add_coach_meta(self):
        '''
        Add the coach meta data
        '''
        ## load pfr data ##
        pfr = self.coach_meta.copy()
        pfr = pfr.groupby(['pfr_coach_name']).head(1)
        ## add headshots ##
        self.compiled_stats = pd.merge(
            self.compiled_stats,
            pfr[[
                'pfr_coach_name', 'pfr_coach_image_url',
                'pfr_coach_tree_hired_by' , 'pfr_coach_tree_hired'
            ]].rename(columns={
                'pfr_coach_name': 'coach',
            }),
            on=['coach'],
            how='left'
        )

    def save_output(self):
        self.compiled_stats.to_csv(
            '{0}/coaches.csv'.format(self.package_loc),
            index=False
        )