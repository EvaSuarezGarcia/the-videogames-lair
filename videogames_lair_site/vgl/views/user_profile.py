from allauth.account.models import EmailAddress
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.views.decorators.http import require_POST
from django.views.generic import TemplateView

from vgl.utils import login_required_or_403


class UserProfile(LoginRequiredMixin, TemplateView):
    template_name = "vgl/user_profile.html"


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
            return HttpResponseRedirect(reverse("vgl:user_profile"))
    except EmailAddress.DoesNotExist:
        pass


def add_email(request):
    # TODO check uniqueness
    email_address = EmailAddress.objects.add_email(request, request.user, request.POST["email"], confirm=False)
    request.user.email = email_address.email
    request.user.save()
    return HttpResponseRedirect(reverse("vgl:user_profile"))


@require_POST
@login_required_or_403
def update_username(request):
    # TODO check uniqueness
    request.user.username = request.POST.get("username")
    request.user.save()

    return HttpResponseRedirect(reverse("vgl:user_profile"))
