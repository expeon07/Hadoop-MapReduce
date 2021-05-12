#!/usr/bin/env python3

from mrjob.job import MRJob


# Implement a MapReduce job that creates a list of followers for each user 
# in the dataset.
class Followers(MRJob):

    # Arg 1: self: the class itself (this)
    # Arg 2: Input key to the map function (here:none)
    # Arg 3: Input value to the map function (here:one line from the input file)
    def mapper(self, _, line):
        # yield (follower, followee) pair
        for follower, followee in line.split():
            yield(followee, follower)


    # Arg 1: self: the class itself (this)
    # Arg 2: Input key to the reduce function (here: the key that was emitted by the mapper)
    # Arg 3: Input value to the reduce function (here: a generator object; something like a
    # sorted list of ALL values associated with the same key)
    def reducer(self, followee, followers):
        followers_list = [follower for follower in followers]
        yield(followee, followers_list)


if __name__ == '__main__':
    Followers.run()
