from datetime import date


class Coach():
    
    def __init__(self, url: str, first_name: str, middle_name:str, last_name: str, birth_date: date):
        self.url = url
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        self.birth_date = birth_date


    def __str__(self):
        return f"\nurl:{self.url}\nfirst_name: {self.first_name}\nmiddle_name: {self.middle_name}\nlast_name: {self.last_name}\nbirth_date: {self.birth_date}\n"

class Player():
    
    def __init__(self, 
                 url: str,
                 first_name: str,
                 last_name: str,
                 number: int, 
                 role: str, 
                 birth_date: date, 
                 growth: int, 
                 weight: int, 
                 transfer_value: int):
        self.url = url
        self.first_name = first_name
        self.last_name = last_name
        self.number = number
        self.role = role
        self.birth_date = birth_date
        self.growth = growth
        self.weight = weight
        self.transfer_value = transfer_value
    
    def __str__(self):
        return f"\nurl: {self.url}\nfirst_name: {self.first_name}\nlast_name: {self.last_name}\nnumber: {self.number}\nrole: {self.role}\nbirth_date: {self.birth_date}\ngrowth: {self.growth}\nweight: {self.weight}\ntransfer_value: {self.transfer_value}\n"


class Team():
    
    
    def __init__(self, id: str, url: str, name: str, players: list[Player], coach: Coach):
        self.id = id
        self.url = url
        self.name = name
        self.players = players
        self.coach = coach
        
    def __str__(self):
        team_str = f'\nTeam:\nid: {self.id}\nurl: {self.url}\nname: {self.name}\n'
        coach_str = f'Coach:\n{self.coach}\n'
        players_str = f'Players:\n'
        for player in self.players:
            players_str += f'{player}'
        
        return team_str + coach_str + players_str
    
    
class Season():
        
        
    def __init__(self, url: str, start_date: date, end_date: date, teams: list[Team]):
        self.url = url
        self.start_date = start_date
        self.end_date = end_date
        self.teams = teams
        
        
    def __str__(self) -> str:
        season_str = f"\nurl: {self.url}\nstart_date: {self.start_date}\nend_date: {self.end_date}\n"
        teams_str = f"Teams:\n"
        for team in self.teams:
            teams_str += f'{team}'
            
        return season_str + teams_str
