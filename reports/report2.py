import mrjob as mr
from mrjob.job import MRJob
from mrjob.step import MRStep
import csv


class MRMostUsedViewed(MRJob):

    def steps(self): # Overwrite the steps function to run a multi-step job
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]

    def mapper_get_words(self, _, row):
        # yield each word in the line
        if 20000 <= row['event_time'] <= 2300:
            yield (row['prog_code'], 1)

    def combiner_count_words(self, program_id, counts):
        # optimization: sum the words we've seen so far
        yield (program_id, sum(counts))

    def reducer_count_words(self, program_id, counts):
        # send all (num_occurrences, program_id) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (sum(counts), program_id)

    # discard the key; it is just None
    def reducer_find_max_word(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        yield max(word_count_pairs)

class MRCountMostViewers(MRJob):
    def mapper_get_words(self, _, row):
        # yield each word in the line
        if row['prog_code'] == most_viewed_program:
            yield (1)

    def reducer_find_max_word(self, cnt):
        yield sum(cnt)

if __name__ == '__main__':
    with open('C:\\Users\\USER\\PycharmProjects\\DataBase_Spring\\reports\\10k_view_data.csv') as file:
        rows = list(csv.reader(file))

    print(rows[1])

    # mr_job = MRMostUsedViewed(args=['--input-file', '10k_view_data.csv'])
    #
    # # Capture the output
    # with mr_job.make_runner() as runner:
    #     runner.run()
    #
    #     # Process the output
    #     for line in runner.stream_output():
    #         key, value = mr_job.parse_output_line(line)
    #         print(f"{value[0]} {value[1]}")  # number_of_viewers program_code
    # most_viewed_program = MRMostUsedViewed.run()
    # viewers = MRCountMostViewers.run()
    #
    # print(f"program: {most_viewed_program} and #viewers is: ")