import json

import requests
from allauth.account.models import EmailAddress
from allauth.account import views as account_views
from allauth.socialaccount.models import SocialAccount
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import HttpResponseRedirect
from django.shortcuts import redirect
from django.urls import reverse_lazy
from django.views.decorators.http import require_POST
from django.views.generic import TemplateView

from vgl.cassandra import CassandraConnectionManager
from vgl.documents import Game
from vgl.utils import login_required_or_403
from videogames_lair_site import settings


class UserProfile(LoginRequiredMixin, TemplateView):
    template_name = "vgl/user_profile.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["steam_success"] = self.request.GET.get("steam_success", "").lower() == "true"

        return context


profile_url = reverse_lazy("vgl:user_profile")


@require_POST
@login_required_or_403
def remove_email(request):
    email = request.POST["email"]
    try:
        email_address = EmailAddress.objects.get(user=request.user, email=email)
        if not request.user.socialaccount_set.all():
            # Can't remove email if there is no Steam account
            pass
        else:
            email_address.delete()
            request.user.email = None
            request.user.save()
            return HttpResponseRedirect(profile_url)
    except EmailAddress.DoesNotExist:
        pass


@require_POST
@login_required_or_403
def add_email(request):
    # TODO check uniqueness
    email_address = EmailAddress.objects.add_email(request, request.user, request.POST["email"], confirm=False)
    request.user.email = email_address.email
    request.user.save()
    return HttpResponseRedirect(profile_url)


def _get_steam_games(user):
    # Get this user's Steam ID
    user_steam_id = user.socialaccount_set.get(provider="steam").extra_data["steamid"]

    # Query Steam for this user's data
    response = json.loads(requests.get("https://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/",
                                       {"key": settings.STEAM_KEY, "steamid": user_steam_id, "format": "json",
                                        "include_played_free_games": True}).content)
    return response.get("response", {}).get("games", [])


def _estimate_ratings(steam_games, games_in_cassandra):
    estimated_ratings = []
    for game in steam_games:
        playtime = game["playtime_forever"]
        cassandra_game = [g for g in games_in_cassandra if g.steam_id == game["appid"]][0]
        if playtime >= cassandra_game.p80:
            rating = 5
        elif playtime >= cassandra_game.p60:
            rating = 4
        elif playtime >= cassandra_game.p40:
            rating = 3
        elif playtime >= cassandra_game.p20:
            rating = 2
        else:
            rating = 1
        estimated_ratings.append((cassandra_game.steam_id, rating))
    return estimated_ratings


def _get_games_from_es(steam_games):
    steam_ids = [game["appid"] for game in steam_games]
    es_games = list(Game.search()
                    .filter("terms", steam_ids=steam_ids)[:100]
                    .execute())
    # Create dict steam id -> vgl ids
    es_games_dict = {}
    for game in es_games:
        for steam_id_str in game.steam_ids:
            steam_id = int(steam_id_str)
            if steam_id in steam_ids:
                es_games_dict.setdefault(steam_id, [])
                es_games_dict[steam_id].append(game.vgl_id)
    return es_games_dict


def _get_vgl_games_to_rate(user_id, es_games_dict, cassandra):
    """Return a set with all vgl ids in es_games_dict except those already rated by the user"""
    user_explicit_rated_games = [rating.game_id for rating in cassandra.get_user_ratings(user_id)
                                 if not rating.estimated]
    games_to_estimate = []
    for steam_id, vgl_ids in es_games_dict.items():
        games_to_estimate.extend([vgl_id for vgl_id in vgl_ids if vgl_id not in user_explicit_rated_games])
    return set(games_to_estimate)


@require_POST
@login_required_or_403
def steam_account_action(request):
    if "action_import" in request.POST:
        base_url = profile_url

        steam_games = _get_steam_games(request.user)

        with CassandraConnectionManager() as cassandra:
            # Filter by games that exist in Cassandra
            cassandra_user_id = request.user.als_user_id
            games_in_cassandra = cassandra.get_steam_games([game["appid"] for game in steam_games])

            steam_games_to_add = [game for game in steam_games if game["appid"] in
                                  [game_cassandra.steam_id for game_cassandra in games_in_cassandra]]

            # Save playtimes to Cassandra
            playtimes_to_add = [(game["appid"], game["playtime_forever"]) for game in steam_games_to_add]
            cassandra.insert_steam_playtimes(cassandra_user_id, playtimes_to_add)

            # Estimate rating for these steam games
            estimated_ratings = _estimate_ratings(steam_games_to_add, games_in_cassandra)

            # Get VGL ids for these steam ids
            es_games_dict = _get_games_from_es(steam_games_to_add)

            # Get VGL games that do not have an explicit rating by the user
            games_to_estimate = _get_vgl_games_to_rate(cassandra_user_id, es_games_dict, cassandra)

            # Save ratings to Cassandra
            ratings_to_add = []
            for rating_tuple in estimated_ratings:
                vgl_ids = es_games_dict[rating_tuple[0]]
                rating = rating_tuple[1]
                ratings_to_add.extend([(vgl_id, rating) for vgl_id in vgl_ids if vgl_id in games_to_estimate])
            cassandra.insert_estimated_ratings(cassandra_user_id, ratings_to_add)

        return redirect(f"{base_url}?steam_success=true")
    else:
        steam_account = SocialAccount.objects.get(user=request.user)
        steam_account.delete()
        return HttpResponseRedirect(profile_url)


@require_POST
@login_required_or_403
def update_username(request):
    # TODO check uniqueness
    request.user.username = request.POST.get("username")
    request.user.save()

    return HttpResponseRedirect(profile_url)


class PasswordChangeView(account_views.PasswordChangeView):
    success_url = profile_url
