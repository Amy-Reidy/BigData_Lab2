#https://github.com/astan54321/PA3/blob/44628868dcc7f00feec9e4c4bdb9391558391ac7/problem2_3.py

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

DATA_RE = re.compile(r"[\w.-]+")


class MRProb2_3(MRJob):


    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ages,
                   reducer=self.reducer_get_avg)
        ]

    def mapper_get_ages(self, _, line):
        # yield age of men
        data = DATA_RE.findall(line)
        if "Male" in data:
            male_ages = float(data[0])
            yield ("Ages of men", male_ages)

    def reducer_get_avg(self, key, values):
        # get max of ages
        max_age = 0
        for val in values:
            if val > max_age:
                max_age = val           
        yield ("The oldest male is", max_age)

if __name__ == '__main__':
    MRProb2_3.run()
