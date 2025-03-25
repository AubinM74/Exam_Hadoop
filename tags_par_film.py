from mrjob.job import MRJob
from mrjob.step import MRStep
class TagsParFilm(MRJob):
	def steps(self):
		return [MRStep(mapper=self.mapper, reducer=self.reducer)]
	def mapper(self, _, line):
		try:
			userId, movieId, tag, timestamp = line.split(",", 3)
			if movieId != "movieId":
				yield movieId, 1
		except Exception:
			pass
	def reducer(self, movieId, counts):
		yield movieId, sum(counts)
if __name__ == '__main__':
	TagsParFilm.run()
