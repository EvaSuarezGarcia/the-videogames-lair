from django.test import TestCase
from model_bakery import baker

from vgl import models
from vgl.documents import Game
from vgl.utils import add_stats_to_games


class UtilsTests(TestCase):

    def test_add_stats_to_games(self):
        game_ids = [1, 2]
        baker.make(models.GameStats, game_id=iter(game_ids), _quantity=len(game_ids))
        games = [Game(vgl_id=game_id) for game_id in game_ids]
        add_stats_to_games(games)
        self.assertTrue(all([game.stats for game in games]))

    def test_add_stats_to_nonexistent_game(self):
        games = [Game(vgl_id=1)]
        add_stats_to_games(games)
        self.assertFalse(all([game.stats for game in games]))