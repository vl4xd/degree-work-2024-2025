from datetime import date, datetime


class CoachID():
    
    def __init__(self, id: str):
        self.id = id
        
    def __str__(self):
        return f'coach_id: {self.id}\n'


class Coach(CoachID):
    
    def __init__(self, id: str, first_name: str, middle_name:str, last_name: str, birth_date: date):
        super().__init__(id)
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        self.birth_date = birth_date


    def __str__(self):
        return super().__str__() + f"id:{self.id}\nfirst_name: {self.first_name}\nmiddle_name: {self.middle_name}\nlast_name: {self.last_name}\nbirth_date: {self.birth_date}\n"

    def __eq__(self, value: 'Coach'):
        if type(value) is not Coach: raise ValueError
        return (self.id==value.id and 
                self.first_name==value.first_name and 
                self.middle_name==value.middle_name and 
                self.last_name==value.last_name and 
                self.birth_date==value.birth_date)


class PlayerID():
    
    def __init__(self, id: str):
        self.id = id

    def __str__(self):
        return f'player_id: {self.id}\n'


class Player(PlayerID):
    
    def __init__(self, 
                 id: str,
                 first_name: str,
                 last_name: str,
                 number: int, 
                 role: str, 
                 birth_date: date, 
                 growth: int, 
                 weight: int, 
                 transfer_value: int):
        super().__init__(id)
        self.first_name = first_name
        self.last_name = last_name
        self.number = number
        self.role = role
        self.birth_date = birth_date
        self.growth = growth
        self.weight = weight
        self.transfer_value = transfer_value
    
    def __str__(self):
        return super().__str__() + f"id: {self.id}\nfirst_name: {self.first_name}\nlast_name: {self.last_name}\nnumber: {self.number}\nrole: {self.role}\nbirth_date: {self.birth_date}\ngrowth: {self.growth}\nweight: {self.weight}\ntransfer_value: {self.transfer_value}\n"
    
    def __eq__(self, value: 'Player'):
        if type(value) is not Player: raise ValueError
        return (self.id==value.id and
                self.first_name==value.first_name and
                self.last_name==value.last_name and
                self.number==value.number and
                self.role==value.role and
                self.birth_date==value.birth_date and
                self.growth==value.growth and
                self.weight==value.weight and
                self.transfer_value==value.transfer_value)


class TeamID():
    
    def __init__(self, id: str):
        self.id = id

    def __str__(self):
        return f'team_id: {self.id}\n'

class Team(TeamID):
    
    def __init__(self, id: str, season_team_id: str, name: str, players: list[Player], coach: Coach):
        super().__init__(id)
        self.season_team_id = season_team_id
        self.name = name
        self.players = players
        self.coach = coach
        
    def __str__(self):
        team_str = f'   \nTeam:\n   id: {self.id}\nseason_team_id: {self.season_team_id}\nname: {self.name}\n'
        coach_str = f'  \nCoach:\n{self.coach}\n'
        players_str = f'    \nPlayers:\n'
        for player in self.players:
            players_str += f'{player}'
        
        return super().__str__() + team_str + coach_str + players_str
    
    def __eq__(self, value: 'Team'):
        if type(value) is not Team: raise ValueError
        return (self.id==value.id and
                self.name==value.name and
                self.season_team_id==value.season_team_id and
                self.coach==value.coach and
                self.players==value.players)
    

class RefereeID():
    
    def __init__(self, id: str):
        self.id = id
        
    def __str__(self):
        return f'referee_id: {self.id}\n'

class Referee(RefereeID):
    
    def __init__(self, id: str, first_name: str, last_name: str):
        super().__init__(id)
        self.first_name = first_name
        self.last_name = last_name

    def __str__(self):
        return super().__str__() + f'referee_fname: {self.first_name}, referee_lname: {self.last_name}'
    
    def __eq__(self, value: 'Referee'):
        if type(value) is not Referee: raise ValueError
        return (self.id==value.id and
                self.first_name==value.first_name and
                self.last_name==value.last_name)
    

class Penalty():
    
    def __init__(self, min: int, plus_min: int, player_id: PlayerID, type: str):
        self.min = min
        self.plus_min = plus_min
        self.player_id = player_id
        self.type = type

    def __str__(self):
        return f'player_id: {self.player_id} min: {self.min} ({self.plus_min}), type: {self.type}'
    
    def __eq__(self, value):
        if type(value) is not Penalty: raise ValueError
        return (self.min==value.min and
                self.plus_min==value.plus_min and
                self.type==value.type and
                self.player_id.id==value.player_id.id)

class Goal():
    
    def __init__(self, min: int, plus_min: int, player_id: PlayerID, player_sub_id: PlayerID, type: str):
        self.min = min
        self.plus_min = plus_min
        self.player_id = player_id
        self.player_sub_id = player_sub_id
        self.type = type
        
    def __str__(self):
        return f'player_id: {self.player_id} sub_player_id: {self.player_sub_id} min: {self.min} ({self.plus_min}), type: {self.type}'
    
    def __eq__(self, value):
        if type(value) is not Goal: raise ValueError
        return (self.min==value.min and
                self.plus_min==value.plus_min and
                self.player_id.id==value.player_id.id and
                self.player_sub_id.id==value.player_sub_id.id and
                self.type==value.type)

class PlayerLineup():
    
    def __init__(self, 
                 player_id: PlayerID, 
                 min_in: int, 
                 plus_min_in: int, 
                 min_out: int, 
                 plus_min_out: int,
                 saves: int):
        self.player_id = player_id
        self.min_in = min_in
        self.plus_min_in = plus_min_in
        self.min_out = min_out
        self.plus_min_out = plus_min_out
        self.saves = saves
    
    def __str__(self):
        return f'player_id: {self.player_id} saves: {self.saves}, min_in: {self.min_in} ({self.plus_min_in}), min_out: {self.min_out} ({self.plus_min_out})'
    
    def __eq__(self, value):
        if type(value) is not PlayerLineup: raise ValueError
        return (self.min_in==value.min_in and
                self.plus_min_in==value.plus_min_in and
                self.min_out==value.min_out and
                self.plus_min_out==value.plus_min_out and
                self.saves==value.saves and
                self.player_id.id==value.player_id.id)

class GameStatPoint():
    
    def __init__(self, stat_name: str, left_team_stat: int, right_team_stat: int):
        self.stat_name = stat_name
        self.left_team_stat = left_team_stat
        self.right_team_stat = right_team_stat

    def __str__(self):
        return f'left: {self.left_team_stat} |name: {self.stat_name}| right: {self.right_team_stat}'
    
    def __eq__(self, value: 'GameStatPoint'):
        if type(value) is not GameStatPoint: raise ValueError
        return self.stat_name == value.stat_name and self.left_team_stat == value.left_team_stat and self.right_team_stat == value.right_team_stat
    
class Game():    
    
    def __init__(self, id: str = None, 
                 date: datetime = None,
                 time: datetime = None,
                 left_season_team_id: str = None, 
                 right_season_team_id: str = None, 
                 tour_number: int = None,
                 is_played: int = None,
                 referee: Referee = None,
                 left_team_goals: list[Goal] = [],
                 right_team_goals: list[Goal] = [],
                 left_team_penalties: list[Penalty] = [],
                 right_team_penalties: list[Penalty] = [],
                 left_coach_id: CoachID = None,
                 right_coach_id: CoachID = None,
                 left_team_lineup: list[PlayerLineup] = [],
                 right_team_lineup: list[PlayerLineup] = [],
                 game_stats: list[GameStatPoint] = [],
                 cur_min: int = None,
                 cur_plus_min: int = None,
                 ):
        self.id = id
        self.date = date
        self.time = time
        self.left_season_team_id = left_season_team_id
        self.right_season_team_id = right_season_team_id
        self.tour_number = tour_number
        self.is_played = is_played
        self.referee = referee
        self.left_team_goals = left_team_goals
        self.right_team_goals = right_team_goals
        self.left_team_penalties = left_team_penalties
        self.right_team_penalties = right_team_penalties
        self.left_coach_id = left_coach_id
        self.right_coach_id = right_coach_id
        self.left_team_lineup = left_team_lineup
        self.right_team_lineup = right_team_lineup
        self.game_stats = game_stats
        self.cur_min = cur_min
        self.cur_plus_min = cur_plus_min
    
    def __iadd__(self, other: 'Game') -> 'Game':
        self.referee = other.referee
        self.left_team_goals = other.left_team_goals
        self.right_team_goals = other.right_team_goals
        self.left_team_penalties = other.left_team_penalties
        self.right_team_penalties = other.right_team_penalties
        self.left_coach_id = other.left_coach_id
        self.right_coach_id = other.right_coach_id
        self.left_team_lineup = other.left_team_lineup
        self.right_team_lineup = other.right_team_lineup
        self.game_stats = other.game_stats
        self.cur_min = other.cur_min
        self.cur_plus_min = other.cur_plus_min
        try:
            self.is_played = other.is_played if other.is_played > 1 else self.is_played
        except: print(f'Некорректная обработка времени игры is_played, id: {self.id}')
        return self
    
    def __eq__(self, value):
        if type(value) is not Game: raise ValueError
        return (self.id==value.id and
                self.cur_min==value.cur_min and
                self.cur_plus_min==value.cur_plus_min and
                self.date==value.date and
                self.time==value.time and
                self.game_stats==value.game_stats and
                self.is_played==value.is_played and
                self.left_coach_id.id==value.left_coach_id.id and
                self.right_coach_id.id==value.right_coach_id.id and
                self.left_season_team_id==value.left_season_team_id and
                self.right_season_team_id==value.right_season_team_id and
                self.left_team_goals==value.left_team_goals and
                self.right_team_goals==value.right_team_goals and
                self.left_team_lineup==value.left_team_lineup and
                self.left_team_penalties==value.left_team_penalties and
                self.right_team_lineup==value.right_team_lineup and
                self.right_team_penalties==value.right_team_penalties and
                self.tour_number==value.tour_number and
                self.referee==value.referee)
    
    def __str__(self):
        res_str = ''
        res_str += str(self.id)
        res_str += str(self.date)
        res_str += str(self.time)
        res_str += str(self.left_season_team_id)
        res_str += str(self.right_season_team_id) 
        res_str += str(self.tour_number)
        res_str += str(self.is_played)
        res_str += str(self.referee)
        for goal in self.left_team_goals:
            res_str += str(goal)
        for goal in self.right_team_goals:
            res_str += str(goal)
        for penalty in self.left_team_penalties:
            res_str += str(penalty)
        for penalty in self.right_team_penalties:
            res_str += str(penalty)
        res_str += str(self.left_coach_id) 
        res_str += str(self.right_coach_id) 
        for lineup in self.left_team_lineup:
            res_str += str(lineup)
        for lineup in self.right_team_lineup:
            res_str += str(lineup)
        for stat in self.game_stats: 
            res_str += str(stat)
        res_str += str(self.cur_min) 
        res_str += str(self.cur_plus_min)
        return res_str
    
    
class Season():
        
        
    def __init__(self, id: str, start_date: date, end_date: date, teams: list[Team], games: list[Game]):
        self.id = id
        self.start_date = start_date
        self.end_date = end_date
        self.teams = teams
        self.games = games
        
        
    def __str__(self) -> str:
        season_str = f"\nSeason:\nid: {self.id}\nstart_date: {self.start_date}\nend_date: {self.end_date}\n"
        teams_str = f"Teams:\n"
        for team in self.teams:
            teams_str += f'{team}'
        games_str = f"Games:\n"
        for game in self.games:
            games_str += f"{game}"
            
        return season_str + teams_str + games_str  
    
    def __eq__(self, value):
        if type(value) is not Season: raise ValueError
        return (self.id==value.id and
                self.start_date==value.start_date and
                self.end_date==value.end_date and
                self.teams==value.teams and
                self.games==value.games)
    