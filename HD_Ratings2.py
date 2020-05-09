from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown (MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   combiner=self.combine_movie_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reduce_sort_ratings)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def combine_movie_ratings(self, key, counts):
        yield key, sum(counts)

    def reducer_count_ratings (self, key, values):
        yield None, (sum(values),key)

    def reduce_sort_ratings(self,_,counts):
        for count, key in sorted(counts, reverse=False):
                yield(int(key),count)

if __name__ == '__main__':
    RatingsBreakdown.run()
