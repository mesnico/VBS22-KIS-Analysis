class Submission:
    
    def __init__(self, memberId, status, teamId, uid, timestamp, itemName):
        self.memberId = memberId
        self.status = status
        self.teamId = teamId
        self.uid = uid
        self.timestamp = timestamp
        self.itemName = itemName
        
    def is_correct(self):
        return self.status == 'CORRECT'
        
    def __lt__(self, other):
         return self.timestamp < other.timestamp