from django.shortcuts import render, get_object_or_404, get_list_or_404
from django.http import HttpResponse
from django.utils import timezone


def index(request):
    return render(request, "vgl/index.html")
