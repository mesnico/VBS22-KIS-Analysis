class TaskCount:
    """
    Count and store the number of Correct or Incorrect to a task
    """

    def __init__(self):
        self.correct = 0
        self.incorrect = 0
        
    def add_status(self, status):
        if status == 'CORRECT':
            self.add_correct()
        elif status == 'WRONG':
            self.add_incorrect()
        
    def add_correct(self):
        self.correct += 1
        
    def add_incorrect(self):
        self.incorrect += 1
        
    def get_incorrect(self):
        if self.incorrect > 0:
            return self.incorrect
        elif self.correct > 0:
            return -1
        else:
            return 0
        
    def __str__(self):
        return 'correct: ' + str(self.correct) + ' incorrect: ' + str(self.incorrect)

class Task:
    """
    Store all the necessary information to a task as well as optional additional information for various data crunching
    """

    def __init__(self, started, ended, duration, position, uid, taskType):
        self.best_logged_rank_video = float('inf')
        self.best_logged_rank_shot = float('inf')
        self.best_logged_time_video = -1
        self.best_logged_time_shot = -1
        self.correct_submission_time = -1
        self.submissions = []
        self.started = started
        self.ended = ended
        self.duration = duration
        self.position = position
        self.uid = uid
        self.taskType = taskType
        self.correct_video = -1
        self.correct_shot = -1
        self.name = ''
        
    def add_name(self, name):
        self.name = name
        
    def add_submission(self, memberId, status, teamId, uid, timestamp, itemName):
        submission = Submission(memberId, status, teamId, uid, timestamp, itemName)
        self.submissions.append(submission)
        self.submissions.sort()
        
    def get_total_submissions(self):
        return len(self.submissions)
    
    def get_correct_submissions(self):
        count = 0
        for sub in self.submissions:
            if sub.is_correct():
                count += 1
        return count
    
    def get_correct_video_submissions(self):
        videos = dict()
        for sub in self.submissions:
            if sub.is_correct():
                videos[sub.itemName] = ''
        return len(videos.keys())
    
    def get_incorrect_or_indeterminate_submissions(self):
        count = 0
        for sub in self.submissions:
            if not sub.is_correct():
                count += 1
        return count
    
    def get_precision(self):
        if self.get_incorrect_or_indeterminate_submissions() == 0 and self.get_correct_submissions() == 0:
            return None
        elif self.get_incorrect_or_indeterminate_submissions() == 0:
            return 1
        return self.get_correct_submissions() / (self.get_correct_submissions() + self.get_incorrect_or_indeterminate_submissions())
            
    def timestamp_within(self, timestamp):
        return timestamp >= self.started and timestamp <= self.ended
    
    def get_logged_time(self, logged_time):
        return (logged_time - self.started) / 1000
    
    def add_new_ranking(self, results):
        for _, res in results.iterrows():
            shotId, videoId, rank, adjusted_logged_time = res['shotId'], res['videoId'], res['rank'], res['adj_logged_time']

            #print('videoId: ' + str(videoId) + ' shotId: ' + str(shotId))
            if shotId is None:
                continue
            if videoId == self.correct_video:
                if rank is None:
                        rank = 0

                if shotId == self.correct_shot:
                    if rank < self.best_logged_rank_shot and (adjusted_logged_time <= self.correct_submission_time or self.correct_submission_time < 0):
                        self.best_logged_rank_shot = rank
                        self.best_logged_time_shot = adjusted_logged_time
                if rank < self.best_logged_rank_video and (adjusted_logged_time <= self.correct_submission_time or self.correct_submission_time < 0):
                    self.best_logged_rank_video = rank
                    self.best_logged_time_video = adjusted_logged_time
                    
    def add_correct_submission_time(self, cst):
        self.correct_submission_time = (cst - self.started) / 1000
        
    def get_name(self):
        return self.name
    
    def add_correct_shot_and_video(self, shotId, videoId):
        self.correct_video = int(videoId)
        self.correct_shot = int(shotId)
        
    def get_rel_info(self, rank_zero=False):
        if rank_zero:
            best_rank_shot = self.best_logged_rank_shot + 1
            best_rank_video = self.best_logged_rank_video + 1
        else:
            best_rank_shot = self.best_logged_rank_shot
            best_rank_video = self.best_logged_rank_video
        if self.best_logged_time_shot == -1:
            best_ranked_time = self.best_logged_time_video
        else:
            best_ranked_time = self.best_logged_time_shot
        return best_rank_shot, best_rank_video, best_ranked_time, self.correct_submission_time
        
    def get_bins(self, nr_bins):
        interval = (self.ended - self.started) / nr_bins
        correct = [0] * nr_bins
        incorrect = [0] * nr_bins
        curr_low = self.started
        curr_high = self.started + interval
        index = 0
        
        for submission in self.submissions:
            if submission.timestamp > curr_high:
                index += 1
                curr_low = curr_high
                curr_high = curr_high + interval
            if submission.status == 'CORRECT':
                correct[index] += 1
            else:
                incorrect[index] += 1
          
        result_bins = [0] * nr_bins
        for i in range(0, nr_bins):
            total = correct[i] + incorrect[i]
            # TODO: How to handle if there are no entries?
            if total == 0:
                result_bins[i] = 1
            else:
                result_bins[i] = correct[i] / total
        return result_bins

    def get_submissions(self):
        submis = dict()

        correct = dict()
        incorrect = dict()

        for s in self.submissions:
            t = int((s.timestamp - self.started)/1000)
            if s.status == 'CORRECT':
                if t in correct:
                    correct[t] += 1
                else:
                    correct[t] = 1
            else:
                if t in incorrect:
                    incorrect[t] += 1
                else:
                    incorrect[t] = 1

        total = dict()
        for i in correct:
            if i in total:
                total[i] += correct[i]
            else:
                total[i] = correct[i]
        for i in incorrect:
            if i in total:
                total[i] += incorrect[i]
            else:
                total[i] = incorrect[i]

        for i in total:
            if i in correct:
                submis[i] = correct[i] / total[i]
            else:
                submis[i] = 0
        return submis
    
    def get_number_of_submissions(self):
        number_of_submissions = dict()
        for submission in self.submissions:
            t = int((submission.timestamp - self.started)/1000)
            if t in number_of_submissions:
                number_of_submissions[t] += 1
            else:
                number_of_submissions[t] = 1
        return number_of_submissions

    def get_number_of_submissions_in_bins(self, nr_bins):
        interval = (self.ended - self.started) / nr_bins
        number_of_submissions = [0] * nr_bins
        curr_low = self.started
        curr_high = self.started + interval
        index = 0
        
        for submission in self.submissions:
            if submission.timestamp > curr_high:
                    index += 1
                    curr_low = curr_high
                    curr_high = curr_high + interval
            number_of_submissions[index] += 1
        return number_of_submissions

    def get_correct_bins(self, nr_bins):
        interval = (self.ended - self.started) / nr_bins
        correct = [0] * nr_bins
        curr_low = self.started
        curr_high = self.started + interval
        index = 0
        for submission in self.submissions:
            if submission.timestamp > curr_high:
                index += 1
                curr_low = curr_high
                curr_high = curr_high + interval
            if submission.status == 'CORRECT':
                correct[index] += 1
        return correct