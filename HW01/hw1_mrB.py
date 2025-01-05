
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

def split_properley (line):
    item = line.split(',')
    date = item[-3]
    air_time = int(item[-2]) if item[-2].isnumeric() else 0

    if item[2] != item[-4]:
        genres = line.split('\"')[1].split(',') if item[0] != 'title' else "Hello World"
    else:
      genres = [item[2]]
    title = item[0].strip()
    return title, genres, air_time, date

class MRWordFrequencyCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_count_genres),
            MRStep(reducer=self.reducer_max)
        ]

    def mapper(self, _, line): 
        title, genres, air_time, date = split_properley(line)
        try:
            if 70000 <= int(air_time) < 90000 and re.search('[jqz]', title.lower()):
                for genre in genres:
                    if genre.strip() in ['Sitcom', 'Talk', 'Politics', 'Spanish', 'Community', 'Martial arts']:
                        ls = [title]
                        ls.extend(genres)
                        yield (tuple(ls), date)
        except Exception as e:
            pass

    def reducer(self, key, dates):
        unique_dates = set(dates)
        yield key, len(unique_dates)

    def reducer_count_genres(self, key, values):
        total_dates = 0
        for dates_count in values:
            total_dates += dates_count
        yield None, (key, total_dates + len(key)-1)# yield key with the sum of genres and total unique dates

    def reducer_max(self, _, values):
        yield max(values, key=lambda x: x[1])



if __name__ == '__main__':
    MRWordFrequencyCount.run()
