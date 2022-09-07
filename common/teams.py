class Teams:
    def __init__(self) -> None:
        self.team_to_id = {}
        self.id_to_team = {}

    def get_teamid_from_teamname(self, name):
        return self.team_to_id[name]

    def get_teamname_from_id(self, id):
        return self.id_to_team[id]

    def add_team(self, name, id):
        self.team_to_id[name] = id
        self.id_to_team[id] = name

    def get_team_ids(self):
        return list(self.id_to_team.keys())

    def get_team_names(self):
        return list(self.team_to_id.keys())