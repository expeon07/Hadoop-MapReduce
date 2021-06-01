#!/usr/bin/env python3
from mrjob.job import MRJob
import heapq

from mrjob.step import MRStep

TOP_FOLLOWERS = 100

# Implement a MapReduce job that creates a list of followees for each user in the dataset.
class MostFollowed(MRJob):

    # Arg 1: self: the class itself (this)
    # Arg 2: Input key to the map function (here:none)
    # Arg 3: Input value to the map function (here:one line from the input file)
    def mapper(self, _, line):
        # yield (followee, 1) pair
        (follower, followee) = line.split()
        yield(followee, 1)


    def combiner(self, followee, follower_count):
        # yield sum of followers
        yield(followee, sum(follower_count))


    def reducer_init(self):
        self.heap = []

    # Arg 1: self: the class itself (this)
    # Arg 2: Input key to the reduce function (here: the key that was emitted by the mapper)
    # Arg 3: Input value to the reduce function (here: a generator object; something like a
    # sorted list of ALL values associated with the same key)
    def reducer(self, followee, follower_count):
        heapq.heappush(self.heap, (sum(follower_count), followee))
        
        if len(self.heap) > TOP_FOLLOWERS:
            heapq.heappop(self.heap)


    def reducer_final(self):
        for (follower_count, followee) in self.heap:
            yield (followee, follower_count)


    # Step 2 — The global TOP_FOLLOWERS needs to run.
    # The mapper outputs "TOP_FOLLOWERS" as the key and (count,word) as the value.
    # We put the count first so that it can be used directly as input to heapq.nlargest()
    def globalTop_mapper(self, followee, follower_count):
        yield "Top " + str(TOP_FOLLOWERS), (follower_count, followee)


    # The reducer ignores the key ("TOP_FOLLOWERS"), 
    # and just finds the largest of the values.
    def globalTop_reducer(self ,_, follower_count):
        for follower_count in heapq.nlargest(TOP_FOLLOWERS, follower_count):
            yield follower_count[1], follower_count[0]


    # TODO count may be wrong
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer,
                   reducer_final=self.reducer_final
                   ),

            MRStep(mapper=self.globalTop_mapper,
                   reducer=self.globalTop_reducer) 
        ]


if __name__ == '__main__':
    MostFollowed.run()
