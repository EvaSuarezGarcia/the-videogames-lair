from django.test import TestCase

from vgl.cassandra import CassandraConnectionManager


class CassandraTests(TestCase):

    def test_get_recommendations_for_user(self):
        user_id = 11347171
        with CassandraConnectionManager() as cassandra:
            recommendations = cassandra.get_recommendations_for_user(user_id)

        self.assertEqual(len(recommendations), 100)
