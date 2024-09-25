import re
import keyword
import json

from http import HTTPMethod
from typing import Any

from django.db import models
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.introspection import FieldInfo
from django.db.backends.utils import CursorWrapper
from django.db.utils import load_backend
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.serializers import ModelSerializer
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


def normalize_table_name(table_name):
    """Translate the table name to a Python-compatible model name."""
    return re.sub(r"[^a-zA-Z0-9]", "", table_name.title())


LOOKUP_SEP = "__"


def normalize_col_name(col_name, used_column_names, is_relation):
    """
    Modify the column name to make it Python-compatible as a field name
    """
    field_params = {}
    field_notes = []

    new_name = col_name.lower()
    if new_name != col_name:
        field_notes.append("Field name made lowercase.")

    if is_relation:
        if new_name.endswith("_id"):
            new_name = new_name.removesuffix("_id")
        else:
            field_params["db_column"] = col_name

    new_name, num_repl = re.subn(r"\W", "_", new_name)
    if num_repl > 0:
        field_notes.append("Field renamed to remove unsuitable characters.")

    if new_name.find(LOOKUP_SEP) >= 0:
        while new_name.find(LOOKUP_SEP) >= 0:
            new_name = new_name.replace(LOOKUP_SEP, "_")
        if col_name.lower().find(LOOKUP_SEP) >= 0:
            # Only add the comment if the double underscore was in the original name
            field_notes.append(
                "Field renamed because it contained more than one '_' in a row."
            )

    if new_name.startswith("_"):
        new_name = "field%s" % new_name
        field_notes.append("Field renamed because it started with '_'.")

    if new_name.endswith("_"):
        new_name = "%sfield" % new_name
        field_notes.append("Field renamed because it ended with '_'.")

    if keyword.iskeyword(new_name):
        new_name += "_field"
        field_notes.append("Field renamed because it was a Python reserved word.")

    if new_name[0].isdigit():
        new_name = "number_%s" % new_name
        field_notes.append("Field renamed because it wasn't a valid Python identifier.")

    if new_name in used_column_names:
        num = 0
        while "%s_%d" % (new_name, num) in used_column_names:
            num += 1
        new_name = "%s_%d" % (new_name, num)
        field_notes.append("Field renamed because of name conflict.")

    if col_name != new_name and field_notes:
        field_params["db_column"] = col_name

    return new_name, field_params, field_notes


def get_field_type(connection, table_name, row):
    """
    Given the database connection, the table name, and the cursor row
    description, this routine will return the given field type name, as
    well as any additional keyword parameters and notes for the field.
    """
    field_params = {}
    field_notes = []

    try:
        field_type = connection.introspection.get_field_type(row.type_code, row)
    except KeyError:
        field_type = "TextField"
        field_notes.append("This field type is a guess.")

    # Add max_length for all CharFields.
    if field_type == "CharField" and row.display_size:
        if (size := int(row.display_size)) and size > 0:
            field_params["max_length"] = size

    if field_type in {"CharField", "TextField"} and row.collation:
        field_params["db_collation"] = row.collation

    if field_type == "DecimalField":
        if row.precision is None or row.scale is None:
            field_notes.append(
                "max_digits and decimal_places have been guessed, as this "
                "database handles decimal fields as float"
            )
            field_params["max_digits"] = (
                row.precision if row.precision is not None else 10
            )
            field_params["decimal_places"] = row.scale if row.scale is not None else 5
        else:
            field_params["max_digits"] = row.precision
            field_params["decimal_places"] = row.scale

    return field_type, field_params, field_notes


def get_meta(
    table_name,
    constraints,
    column_to_field_name,
    is_view,
    is_partition,
    comment,
):
    """
    Return a sequence comprising the lines of code necessary
    to construct the inner Meta class for the model corresponding
    to the given database table name.
    """
    unique_together = []
    has_unsupported_constraint = False
    for params in constraints.values():
        if params["unique"]:
            columns = params["columns"]
            if None in columns:
                has_unsupported_constraint = True
            columns = [
                x for x in columns if x is not None and x in column_to_field_name
            ]
            if len(columns) > 1:
                unique_together.append(
                    str(tuple(column_to_field_name[c] for c in columns))
                )
    if is_view:
        managed_comment = "  # Created from a view. Don't remove."
    elif is_partition:
        managed_comment = "  # Created from a partition. Don't remove."
    else:
        managed_comment = ""
    meta = [""]
    if has_unsupported_constraint:
        meta.append("    # A unique constraint could not be introspected.")
    meta += [
        "    class Meta:",
        "        managed = False%s" % managed_comment,
        "        db_table = %r" % table_name,
    ]
    if unique_together:
        tup = "(" + ", ".join(unique_together) + ",)"
        meta += ["        unique_together = %s" % tup]
    if comment:
        meta += [f"        db_table_comment = {comment!r}"]
    return meta


class QueryMetaClass(type):
    def __new__(cls, name, bases, attrs):
        print(f"Creating class {name} with metaclass {cls.__name__}")
        return super().__new__(cls, name, bases, attrs)


FIELD_MAPPING_CLASS = {
    "CharField": models.CharField,
    "IntegerField": models.IntegerField,
    "BooleanField": models.BooleanField,
    "DateTimeField": models.DateTimeField,
    "ForeignKey": models.ForeignKey,
    "AutoField": models.AutoField,
    "BigAutoField": models.BigAutoField,
    "TextField": models.TextField,
}


class DatabaseScanner:
    def __init__(self, db: BaseDatabaseWrapper, cursor: CursorWrapper) -> None:
        self.__db = db
        self.__cursor = cursor

        self.__model_classes = dict[str, type[models.Model]]()
        self.__model_serializer_classess = dict[str, type[ModelSerializer]]()

    def get_models(self):
        return self.__model_classes

    def get_model_serializer_classess(self):
        return self.__model_serializer_classess

    def __get_table_primary_key_column_name(self, table: str) -> str | None:
        primary_key_columns = self.__db.introspection.get_primary_key_columns(
            self.__cursor, table
        )
        return primary_key_columns[0] if primary_key_columns else None

    def __get_table_relations(self, table: str) -> dict:
        try:
            relations = self.__db.introspection.get_relations(self.__cursor, table)
        except NotImplementedError:
            relations = {}
        return relations

    def __get_unique_columns(self, table: str):
        try:
            constraints = self.__db.introspection.get_constraints(self.__cursor, table)
        except NotImplementedError:
            constraints = {}
        return [
            c["columns"][0]
            for c in constraints.values()
            if c["unique"] and len(c["columns"]) == 1
        ]

    def __create_drf_serializer_cls(
        self, model_cls: type[models.Model], fields_to_serialize: list[str]
    ) -> type[ModelSerializer]:
        class Meta:
            model = model_cls
            fields = fields_to_serialize

        serializer_fields = {}
        serializer_fields["__module__"] = ""
        serializer_fields["Meta"] = Meta

        return type(
            f"{str(model_cls)}Serializer", (ModelSerializer,), serializer_fields
        )

    def scan_all_tables(self):
        types = {
            "t",  # Tables
            "p",  # Partitions
            "v",  # Views
        }
        table_info = self.__db.introspection.get_table_list(self.__cursor)
        table_info = {info.name: info for info in table_info if info.type in types}

        for table in sorted(name for name in table_info):
            primary_key_column = self.__get_table_primary_key_column_name(table)
            unique_columns = self.__get_unique_columns(table)

            model_name = normalize_table_name(table)

            relations = self.__get_table_relations(table)
            # Holds column names used in the table so far
            used_column_names = []
            # Maps column names to names of model fields
            column_to_field_name = {}
            # Holds foreign relations used in the table.
            used_relations = set()

            known_models = [model_name]
            fields: dict[str, Any] = {}

            for row in self.__db.introspection.get_table_description(
                self.__cursor,
                table,
            ):
                row: FieldInfo = row
                column_name = row.name
                is_relation = column_name in relations

                # Holds Field parameters such as 'db_column'.
                extra_params = {}

                att_name, params, _ = normalize_col_name(
                    column_name, used_column_names, is_relation
                )

                extra_params.update(params)
                used_column_names.append(att_name)

                column_to_field_name[column_name] = att_name

                if column_name == primary_key_column:
                    extra_params["primary_key"] = True
                elif column_name in unique_columns:
                    extra_params["unique"] = True

                if is_relation:
                    ref_db_column, ref_db_table = relations[column_name]
                    if extra_params.pop("unique", False) or extra_params.get(
                        "primary_key"
                    ):
                        rel_type = "OneToOneField"
                    else:
                        rel_type = "ForeignKey"
                        ref_pk_column = self.__db.introspection.get_primary_key_column(
                            self.__cursor, ref_db_table
                        )
                        if ref_pk_column and ref_pk_column != ref_db_column:
                            extra_params["to_field"] = ref_db_column
                    rel_to = (
                        "self"
                        if ref_db_table == table
                        else normalize_table_name(ref_db_table)
                    )
                    if rel_to in known_models:
                        field_type = "%s(%s" % (rel_type, rel_to)
                    else:
                        field_type = "%s('%s'" % (rel_type, rel_to)

                    if rel_to in used_relations:
                        extra_params["related_name"] = "%s_%s_set" % (
                            model_name.lower(),
                            att_name,
                        )

                    used_relations.add(rel_to)
                    print(
                        "relation",
                        rel_to,
                        "ref table",
                        ref_db_table,
                        "ref on",
                        ref_db_column,
                        "local_column",
                        column_name,
                        "rel",
                        rel_type,
                        rel_to,
                    )
                    # TODO: add foregin key type

                else:
                    field_type, field_params, _ = get_field_type(self.__db, table, row)
                    extra_params.update(field_params)

                # TODO: field construct on own func
                field = FIELD_MAPPING_CLASS.get(field_type)
                if not field:
                    # print(
                    #     "Not found field mapper",
                    #     model_name,
                    #     column_name,
                    #     field_type,
                    #     extra_params,
                    # )
                    continue

                fields[column_name] = field(**extra_params)

                # print(model_name, column_name, field_type, extra_params)

            # print(fields)

            fields_to_serialize = list(map(lambda q: q[0], fields.items()))

            fields["__module__"] = ""

            class Meta:
                db_table = table
                managed = False
                app_label = ""

            fields["Meta"] = Meta
            django_model_class: type[models.Model] = type(
                model_name, (models.Model,), fields
            )

            django_drf_model_serializer = self.__create_drf_serializer_cls(
                django_model_class, fields_to_serialize
            )

            self.__model_serializer_classess[model_name] = django_drf_model_serializer
            self.__model_classes[model_name] = django_model_class

            # print(django_model_class)
            # print("result of", django_model_class.objects.all())
        pass


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
            scanner = DatabaseScanner(db, cursor)
            scanner.scan_all_tables()

            data = []
            serializers = scanner.get_model_serializer_classess()
            print("Found N model serializers:", len(serializers))

            for model, cls in scanner.get_models().items():
                serializer = serializers.get(model)
                if not serializer:
                    print(f"Not found {model} model serializer")
                    continue

                result = cls.objects.all()

                s = serializer(result, many=True)

                data.extend(s.data)

        db.close()

        return Response(data, status=status.HTTP_200_OK)
