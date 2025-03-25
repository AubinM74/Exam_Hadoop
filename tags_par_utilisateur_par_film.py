from mrjob.job import MRJob
from mrjob.step import MRStep
class TagsParUtilisateurParFilm(MRJob):
	def steps(self):
		return [MRStep(mapper=self.mapper, reducer=self.reducer)]
	def mapper(self, _, line):
		try:
			userId, movieId, tag, timestamp = line.split(",", 3)
			if userId != "userId":
				key = "%s:%s" % (movieId, userId)
				yield key, 1
		except Exception:
			pass
	def reducer(self, key, values):
		yield key, sum(values)
if __name__ == '__main__':
	TagsParUtilisateurParFilm.run()
