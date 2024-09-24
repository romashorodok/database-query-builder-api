from django.db import models


class DataSource(models.Model):
    name = models.TextField()
    address = models.TextField()
    port = models.IntegerField()
    user = models.TextField()
    # Is it possible restore password from bcrypt hash?
    password = models.TextField()


class Author(models.Model):
    name = models.CharField(max_length=100)

    def __str__(self):
        return self.name


class Book(models.Model):
    title = models.CharField(max_length=200)

    # class Meta:        managed = False        db_table = 'auth_permission'        unique_together = (('content_type', 'codename'),)

    authors = models.ManyToManyField(Author, related_name="books")  # Many-to-many field

    def __str__(self):
        return self.title
