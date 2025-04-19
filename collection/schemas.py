from datetime import date


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


class Referee():
    
    def __init__(self, id: str, first_name: str, last_name: str):
        self.id = id
        self.first_name = first_name
        self.last_name = last_name


class Goal():
    
    
    def __init__(self, min: int, player: PlayerID, player_sub: PlayerID, type: str):
        self.min = min
        self.player = player
        self.player_sub = player_sub
        self.type = type


class Game():    
    
    def __init__(self, id: str = None, 
                 left_team_id: str = None, 
                 right_team_id: str = None, 
                 referee: Referee = None
                 ):
        pass
    
    
    def __str__(self):
        pass

class Calendar():
    
    
    def __init__(self):
        pass
    
    
    def __str__(self):
        pass
    