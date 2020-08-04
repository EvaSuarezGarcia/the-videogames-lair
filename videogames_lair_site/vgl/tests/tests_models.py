from django.test import TestCase
from vgl.models import AgeRating, Concept, user_ids_seq


class ALSUserTests(TestCase):

    def test_save_with_no_id(self):
        """
        If an ALSUser is saved without an ID, it is drawn from a sequence. Moreover, the next ALSUser
        will have a consecutive ID.
        """

        als_user = AgeRating.objects.create(gb_id=1, name="test")
        als_user2 = Concept.objects.create(gb_id=2, name="test2")

        self.assertIsNotNone(als_user.user_id)
        self.assertIsNotNone(als_user2.user_id)
        self.assertEqual(als_user.user_id + 1, als_user2.user_id)

    def test_save_with_id(self):
        """
        If an ALSUser is saved with an ID, its ID remains untouched and the user IDs sequence doesn't advance.
        """

        last_value = user_ids_seq.get_last_value()
        als_user = AgeRating.objects.create(user_id=3, gb_id=1, name="test")

        self.assertEqual(als_user.user_id, 3)
        self.assertEqual(last_value, user_ids_seq.get_last_value())
