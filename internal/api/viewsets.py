from typing import Any
from django.db.utils import load_backend
from django.db.backends.base.base import BaseDatabaseWrapper

from rest_framework import status
from rest_framework.decorators import action
from http import HTTPMethod

from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet, ViewSet

from database_query_builder_api.settings import TIME_ZONE
from internal.api.models import DataSource

from .serializers import DataSourceSerializer


class DataSourceViewSet(ModelViewSet):
    queryset = DataSource.objects.all()

    serializer_class = DataSourceSerializer


DATABASE_ENGINES = {
    "postgres": "django.db.backends.postgresql",
}


class QueryViewSet(ViewSet):
    @action(methods=[HTTPMethod.GET], detail=False)
    def select(self, _, data_source: str):
        data_source_model = DataSource.objects.filter(name=data_source).first()
        print(data_source_model)
        if not data_source_model:
            raise ValueError("Not found datasource")

        alias = "test-postgres"

        engine = DATABASE_ENGINES["postgres"]

        backend = load_backend(engine)
        settings: dict[str, Any] = {
            "ENGINE": engine,
            "NAME": data_source_model.name,
            "HOST": data_source_model.address,
            "PORT": data_source_model.port,
            "USER": data_source_model.user,
            "PASSWORD": data_source_model.password,
            "TIME_ZONE": TIME_ZONE,
        }

        db: BaseDatabaseWrapper = backend.DatabaseWrapper(
            settings,
            alias,
        )

        # From django.db.utils.ConnectionHandler.configure_settings
        db.settings_dict = settings
        db.settings_dict["ATOMIC_REQUESTS"] = False
        db.settings_dict["AUTOCOMMIT"] = False
        db.settings_dict["CONN_MAX_AGE"] = 0
        db.settings_dict["CONN_HEALTH_CHECKS"] = False
        db.settings_dict["OPTIONS"] = {}

        with db.cursor() as cursor:
            types = {
                "t",  # Tables
                "p",  # Partitions
                "v",  # Views
            }
            table_info = db.introspection.get_table_list(cursor)
            table_info = {info.name: info for info in table_info if info.type in types}

            print(table_info)

        data = [
            {"id": 1, "name": "Item 1", "description": "Description 1"},
            {"id": 2, "name": "Item 2", "description": "Description 2"},
        ]
        return Response(data, status=status.HTTP_200_OK)
