from datetime import date, datetime


class CoachID():
    
    def __init__(self, id: str):
        self.id = id


class Coach(CoachID):
    
    def __init__(self, id: str, first_name: str, middle_name:str, last_name: str, birth_date: date):
        super().__init__(id)
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        self.birth_date = birth_date


    def __str__(self):
        return f"       \nid:{self.id}\nfirst_name: {self.first_name}\nmiddle_name: {self.middle_name}\nlast_name: {self.last_name}\nbirth_date: {self.birth_date}\n"


class PlayerID():
    
    def __init__(self, id: str):
        self.id = id


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
        return f"       \nid: {self.id}\nfirst_name: {self.first_name}\nlast_name: {self.last_name}\nnumber: {self.number}\nrole: {self.role}\nbirth_date: {self.birth_date}\ngrowth: {self.growth}\nweight: {self.weight}\ntransfer_value: {self.transfer_value}\n"


class TeamID():
    
    def __init__(self, id: str):
        self.id = id


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
        
        return team_str + coach_str + players_str
    
    
class Season():
        
        
    def __init__(self, id: str, start_date: date, end_date: date, teams: list[Team]):
        self.id = id
        self.start_date = start_date
        self.end_date = end_date
        self.teams = teams
        
        
    def __str__(self) -> str:
        season_str = f"\nSeason:\nid: {self.id}\nstart_date: {self.start_date}\nend_date: {self.end_date}\n"
        teams_str = f"Teams:\n"
        for team in self.teams:
            teams_str += f'{team}'
            
        return season_str + teams_str

class RefereeID():
    
    def __init__(self, id: str):
        self.id = id

class Referee(RefereeID):
    
    def __init__(self, id: str, first_name: str, last_name: str):
        super().__init__(id)
        self.first_name = first_name
        self.last_name = last_name


class Penalty():
    
    def __init__(self, min: int, plus_min: int, player_id: PlayerID, type: str):
        self.min = min
        self.plus_min = plus_min
        self.player_id = player_id
        self.type = type


class Goal():
    
    def __init__(self, min: int, plus_min: int, player_id: PlayerID, player_sub_id: PlayerID, type: str):
        self.min = min
        self.plus_min = plus_min
        self.player_id = player_id
        self.player_sub_id = player_sub_id
        self.type = type

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

class GameStatPoint():
    
    def __init__(self, stat_name: str, left_team_stat: int, right_team_stat: int):
        self.stat_name = stat_name
        self.left_team_stat = left_team_stat
        self.right_team_stat = right_team_stat

class Game():    
    
    def __init__(self, id: str = None, 
                 date: datetime = None,
                 time: datetime = None,
                 left_team_id: str = None, 
                 right_team_id: str = None, 
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
                 ):
        self.id = id
        self.date = date
        self.time = time
        self.left_team_id = left_team_id
        self.right_team_id = right_team_id
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
    
    
    def __str__(self):
        pass

class Calendar():
    
    
    def __init__(self):
        pass
    
    
    def __str__(self):
        pass
    