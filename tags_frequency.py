from mrjob.job import MRJob
from mrjob.step import MRStep
class TagsFrequency(MRJob):
	def steps(self):
		return [MRStep(mapper=self.mapper, reducer=self.reducer)]
	def mapper(self, _, line):
		try:
			userId, movieId, tag, timestamp = line.split(",", 3)
			if tag != "tag":
				yield tag.strip().lower(), 1
		except Exception:
			pass
	def reducer(self, tag, counts):
		yield tag, sum(counts)
if __name__ == '__main__':
	TagsFrequency.run()
