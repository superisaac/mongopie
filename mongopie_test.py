# A simple example on mongopie
# User Vote and Tag

import mongopie

mongopie.set_defaultdb('localhost', 27017, 'pietest')

class UserTag(mongopie.Model):
    user = mongopie.StringField()
    tag = mongopie.StringField()
    count = mongopie.IntegerField(default=0)

    @classmethod
    def add_tag(cls, vote):
        ut = cls.find_one(user=vote.votee,
                          tag=vote.tag)
        if not ut:
            ut = cls()
            ut.user = vote.votee
            ut.tag = vote.tag
        ut.count += 1
        ut.save()
        return ut            
        
class Vote(mongopie.Model):
    voter = mongopie.StringField()
    votee = mongopie.StringField()
    tag = mongopie.StringField()
    created_at = mongopie.DateTimeField(auto_now=True)

    def on_created(self):
        UserTag.add_tag(self)
    
def make_vote(voter, votee, tag):
    v = Vote()
    v.voter = voter
    v.votee = votee
    v.tag = tag
    v.save()
    return v

def test():
    make_vote('Tom', 'Jack', 'Hacking')
    make_vote('Jerry', 'Jack', 'Food')
    make_vote('Jerry', 'Jack', 'Hacking')
    for ut in UserTag.find(user='Jack').sort('tag').find(tag='Hacking'):
        print ut.get_dict()

if __name__ == '__main__':
    test()
