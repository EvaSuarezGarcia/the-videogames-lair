from django.test import TestCase

from vgl import cassandra


class CassandraTests(TestCase):

    def test_get_recommendations_for_user(self):
        user_id = 11347171
        recommendations = cassandra.get_recommendations_for_user(user_id)
        ratings = cassandra.get_user_ratings(user_id)

        self.assertGreater(len(recommendations), 0)
        self.assertLessEqual(len(recommendations), 100)
        self.assertFalse(any(rating in recommendations for rating in ratings))
