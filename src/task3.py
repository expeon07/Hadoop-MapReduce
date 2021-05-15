#!/usr/bin/env python3


from mrjob.job import MRJob


# Implement a MapReduce job that creates a list of followees for each user in the dataset.
class MostFollowed(MRJob):

    # Arg 1: self: the class itself (this)
    # Arg 2: Input key to the map function (here:none)
    # Arg 3: Input value to the map function (here:one line from the input file)
    def mapper(self, _, line):

        # TODO trailing zeros?

        # yield (followee, 1) pair
        (follower, followee) = line.split()
        yield(int(followee), 1)

    def combiner(self, followee, follower_count):

        # yield sum of followers
        yield(followee, sum(follower_count))


    # Arg 1: self: the class itself (this)
    # Arg 2: Input key to the reduce function (here: the key that was emitted by the mapper)
    # Arg 3: Input value to the reduce function (here: a generator object; something like a
    # sorted list of ALL values associated with the same key)
    def reducer(self, followee, follower_count):

        # TODO get only top 100
        top_followed = []
        yield(followee, sum(follower_count))


if __name__ == '__main__':
    MostFollowed.run()
