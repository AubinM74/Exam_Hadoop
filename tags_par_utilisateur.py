from mrjob.job import MRJob
from mrjob.job import MRJob
class TagsParUtilisateur(MRJob):
	def steps(self):
		return [MRStep(mapper=self.mapper, reducer=self.reducer)]
	def mapper(self, _, line):
		try:
			userId, movieId, tag, timestamp = line.split(",", 3)
			if userId != "userId":
				yield userId, 1
		except Exception:
			pass
	def reducer(self, userId, counts):
		yield userId, sum(counts)
if __name__ == '__main__':
	TagsParUtilisateur.run()
