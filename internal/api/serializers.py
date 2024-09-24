from typing import override
from rest_framework.serializers import ModelSerializer

from internal.api.models import DataSource


class DataSourceSerializer(ModelSerializer):
    class Meta:
        model = DataSource
        fields = (
            "name",
            "address",
            "port",
            "user",
            "password",
        )

    @override
    def create(self, validated_data) -> DataSource:
        data_source = DataSource(**validated_data)
        data_source.save()
        return data_source
