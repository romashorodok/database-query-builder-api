from django.db import models


class DataSource(models.Model):
    name = models.TextField()
    address = models.TextField()
    port = models.IntegerField()
    user = models.TextField()
    # Is it possible restore password from bcrypt hash?
    password = models.TextField()
