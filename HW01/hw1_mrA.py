
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

def split_properley (line):
    item = line.split(',')
    date = item[-3] # taking third item from the righ to left on the row, which represents the date
    air_time = int(item[-2]) if item[-2].isnumeric() else 0 #getting the integer value fo the item second from the right side of the row

    if item[2] != item[-4]:# checking if there are multiple genres or only 1.
        genres = line.split('\"')[1].split(',') if item[0] != 'title' else "Hello World"
    else:# There is only one genre for this tuple
      genres = [item[2]]
    title = item[0].strip()
    return title, genres, air_time, date

class MRWordFrequencyCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_count_genres),
            #MRStep(reducer=self.reducer_max)
        ]

    def mapper(self, _, line):
        title, genres, air_time, date = split_properley(line)# get relevant data from the tuple
        try:
            if 70000 <= int(air_time) < 90000 and re.search('[jqz]', title.lower()):# filter out by airtime and that title contains one of the letters j,q,z
                for genre in genres:
                    if genre.strip() in ['Sitcom', 'Talk', 'Politics', 'Spanish', 'Community', 'Martial arts']:
                        ls = [title]
                        ls.extend(genres)
                        yield (tuple(ls), date)# if one of the genres is in the list then we will move this tuple reformatted to the next step
        except Exception as e:
            pass

    def reducer(self, key, dates):
        yield key, len(set(dates))# reduce the value so it contains the number of unique dates

    def reducer_count_genres(self, key, values):
        total_dates = 0
        for dates_count in values:
            total_dates += dates_count
        yield key, (total_dates, len(key)-1)# counting number of dates per key, counting number of genres



if __name__ == '__main__':
    MRWordFrequencyCount.run()
