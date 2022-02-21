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
        # yield age of women
        data = DATA_RE.findall(line)
        if "Female" in data:
            female_ages = float(data[0])
            yield ("Ages of women", female_ages)

    def reducer_get_avg(self, key, values):
        # get max of ages
        min_age = 100
        for val in values:
            if val < min_age:
                min_age = val           
        yield ("The youngest female is", min_age)

if __name__ == '__main__':
    MRProb2_3.run()
