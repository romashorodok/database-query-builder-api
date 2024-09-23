from django.http import HttpResponse


def index(request):
    return HttpResponse(b"Hello, world. You're at the api index.")
