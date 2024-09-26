import re
import keyword
from dataclasses import dataclass

from http import HTTPMethod
from typing import Any

from django.db import models
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.introspection import FieldInfo
from django.db.backends.utils import CursorWrapper
from django.db.utils import load_backend
from django.forms import model_to_dict
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.serializers import ModelSerializer
from rest_framework.viewsets import ModelViewSet, ViewSet

from database_query_builder_api.settings import TIME_ZONE
from internal.api.models import Author, Book, DataSource

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


class QueryPrimitiveFieldModel:
    def __init__(self, field_type: str, options: dict[str, Any]) -> None:
        field_cls = FIELD_MAPPING_CLASS.get(field_type)
        if not field_cls:
            raise ValueError("Query primitive field model must be a mapped class")

        self.__field = field_cls
        self.__options = options

    def make_field(self) -> models.Field:
        return self.__field(**self.__options)


class QueryForeginModel:
    def __init__(
        self, rel_type: str, rel_to: str, db_column: str, ref_db_column: str
    ) -> None:
        field_cls = FIELD_MAPPING_CLASS.get(rel_type)
        if not field_cls or not issubclass(field_cls, models.ForeignKey):
            raise ValueError("Query foregin model must be a models.ForeignKey")

        self.__field = field_cls
        self.__rel_to = rel_to
        self.__options: dict[str, Any] = {
            # "to": rel_to,
            "db_column": db_column,
            "to_field": ref_db_column,
        }

        self.serializer: type[ModelSerializer] | None = None

    @property
    def rel_to(self) -> str:
        return self.__rel_to

    def make_field(self) -> models.ForeignKey | None:
        if self.__options.get("to"):
            return self.__field(on_delete=models.DO_NOTHING, **self.__options)
        return None

    def update_foregin_model(self, query_model: type[models.Model]):
        self.__options["to"] = query_model

    def update_foregin_model_serializer(self, serializer: type[ModelSerializer]):
        self.serializer = serializer


class QueryModel:
    def __init__(
        self,
        model_name: str,
        table_name: str,
    ) -> None:
        self.__model_name = model_name
        self._table_name = table_name

        self.__query_foregin_model = dict[str, QueryForeginModel]()
        self.__query_primitive_model = dict[str, QueryPrimitiveFieldModel]()

    def add_query_foregin_model(
        self, field_name: str, query_foregin_model: QueryForeginModel
    ):
        self.__query_foregin_model[field_name] = query_foregin_model

    def get_query_foregin_models(self):
        return self.__query_foregin_model

    def get_query_foregin_models_fields(self) -> list[str]:
        return list(map(lambda q: q[0], self.__query_foregin_model.items()))

    def add_query_primitive_field_model(
        self,
        field_name: str,
        query_primitive_model: QueryPrimitiveFieldModel,
    ):
        self.__query_primitive_model[field_name] = query_primitive_model

    def __get_fields(self) -> dict[str, Any]:
        fields = {}

        for (
            primitive_field_name,
            primitive_field,
        ) in self.__query_primitive_model.items():
            fields[primitive_field_name] = primitive_field.make_field()

        for foregin_field_name, foregin_field in self.__query_foregin_model.items():
            if (field := foregin_field.make_field()) and field:
                fields[foregin_field_name] = field

        return fields

    def __get_serializer_fields(self) -> tuple[list[str], dict[str, Any]]:
        fields_to_serialize = list(self.__query_primitive_model.keys())
        serializer_fields = {}

        for foregin_field_name, foregin_field in self.__query_foregin_model.items():
            serializer = foregin_field.serializer
            if serializer:
                serializer_fields[foregin_field_name] = serializer()
                fields_to_serialize.append(foregin_field_name)
                print(f"serializer of {foregin_field_name}", serializer)

        return (fields_to_serialize, serializer_fields)

    def get_django_model(self) -> type[models.Model]:
        fields = self.__get_fields()
        fields["__module__"] = ""

        class Meta:
            db_table = self._table_name
            managed = False
            app_label = ""

        fields["Meta"] = Meta

        return type(self.__model_name, (models.Model,), fields)

    def get_drf_serializer(
        self, model_cls: type[models.Model]
    ) -> type[ModelSerializer]:
        serializer_field_names, serializer_fields = self.__get_serializer_fields()

        class Meta:
            model = model_cls
            fields = serializer_field_names

        serializer_fields["__module__"] = ""
        serializer_fields["Meta"] = Meta

        return type(
            f"{self.__model_name}Serializer",
            (ModelSerializer,),
            serializer_fields,
        )

    #
    # def add_related_foregin_name(
    #     self,
    #     foregin_key_column: str,
    #     mapped_field_name: str,
    #     mapped_model_name: str,
    #     model_table_name: str,
    # ):
    #     self.__query_foregin_keys.append(
    #         QueryForeginKey(
    #             foregin_key_column,
    #             mapped_field_name,
    #             mapped_model_name,
    #             model_table_name,
    #         ),
    #     )
    #
    # def add_model_field_name(self, model_field_name: str):
    #     # self.__model_field_names.append(model_field_name)
    #     self.__model_field_names.add(model_field_name)
    #
    # def get_query_foregin_key_names(self):
    #     return self.__query_foregin_keys
    #
    # def get_model_field_names(self):
    #     return self.__model_field_names


class DatabaseScanner:
    def __init__(self, db: BaseDatabaseWrapper, cursor: CursorWrapper) -> None:
        self.__db = db
        self.__cursor = cursor

        # self.__model_classes = dict[str, type[models.Model]]()
        # self.__model_serializer_classess = dict[str, type[ModelSerializer]]()
        self.__query_projections = dict[str, QueryModel]()

    # def get_models(self):
    #     return self.__model_classes
    #
    # def get_model_serializer_classess(self):
    #     return self.__model_serializer_classess

    def get_query_projections(self):
        return self.__query_projections

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

    def link_foregin_keys(self):
        projections = self.__query_projections

        for _, query_model in projections.items():
            foregins = query_model.get_query_foregin_models()

            for _, foregin_model in foregins.items():
                foregin_model_name = foregin_model.rel_to
                if (model := projections.get(foregin_model_name)) and model:
                    django_model = model.get_django_model()
                    foregin_model.update_foregin_model(django_model)
                    foregin_model.update_foregin_model_serializer(
                        model.get_drf_serializer(django_model)
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
            query_model = QueryModel(model_name, table)

            relations = self.__get_table_relations(table)
            # Holds column names used in the table so far
            used_column_names = []
            # Maps column names to names of model fields
            column_to_field_name = {}
            # Holds foreign relations used in the table.
            # used_relations = set()

            known_models = [model_name]

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

                    # if rel_to in used_relations:
                    #     extra_params["related_name"] = "%s_%s_set" % (
                    #         model_name.lower(),
                    #         att_name,
                    #     )

                    # used_relations.add(rel_to)

                    # print(
                    #     "relation",
                    #     rel_to,
                    #     "ref table",
                    #     ref_db_table,
                    #     "ref on",
                    #     ref_db_column,
                    #     # "local_column",
                    #     # column_name,
                    #     "rel",
                    #     rel_type,
                    #     rel_to,
                    # )
                    # print("relation", extra_params)


                    query_model.add_query_foregin_model(
                        att_name,
                        QueryForeginModel(
                            rel_type,
                            rel_to,
                            column_name,
                            ref_db_column,
                        ),
                    )
                    continue

                else:
                    field_type, field_params, _ = get_field_type(self.__db, table, row)
                    extra_params.update(field_params)

                query_model.add_query_primitive_field_model(
                    column_name,
                    QueryPrimitiveFieldModel(
                        field_type,
                        extra_params,
                    ),
                )

            self.__query_projections[model_name] = query_model


class QueryViewSet(ViewSet):
    @action(methods=[HTTPMethod.GET], detail=False)
    def select(self, _, data_source: str):
        data_source_model = DataSource.objects.filter(name=data_source).first()
        print(data_source_model)
        if not data_source_model:
            raise ValueError("Not found datasource")

        alias = "test-postgres"

        engine = DATABASE_ENGINES["postgres"]

        # author = Author.objects.create(name="J.K. Rowling")
        # book = Book.objects.create(title="Harry Potter and the Philosopher's Stone")
        # book.authors.add(author)
        # book.save()

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
            scanner.link_foregin_keys()

            data = []
            projections = scanner.get_query_projections()

            for _, query_model in projections.items():
                django_model = query_model.get_django_model()
                drf_serializer = query_model.get_drf_serializer(django_model)

                related_fields = query_model.get_query_foregin_models_fields()
                result = django_model.objects.select_related(*related_fields).all()

                for r in result:
                    print(model_to_dict(r))
                    for i in related_fields:
                        attr = getattr(r, i)
                        if attr:
                            print("relation attr type", type(attr))
                            print("has relation attr", model_to_dict(attr))

                s = drf_serializer(result, many=True)
                data.extend(s.data)

                # pass

            # print("Found N model serializers:", len(serializers))

            # for model_name, cls in scanner.get_models().items():
            #     serializer = serializers.get(model_name)
            #     if not serializer:
            #         print(f"Not found {model_name} model serializer")
            #         continue
            #
            #     projection = projections.get(model_name)
            #     if not projection:
            #         print(f"Not found {model_name} model projection")
            #         continue
            #
            #     foregin_key = projection.get_query_foregin_key_names()
            #     print("relation", foregin_key)
            #     foregin_key_colums = map(lambda q: q.foregin_key_column, foregin_key)
            #
            #     tables = list[str]()
            #     table_select = list[str]()
            #
            #     # for fk in foregin_key_names:
            #     #     mapped_projection = projections.get(fk.mapped_model_name)
            #     #     if not mapped_projection:
            #     #         continue
            #     #
            #     #     tables.append(fk.model_table_name)
            #     #
            #     #     names = mapped_projection.get_model_field_names()
            #     #     for select in names:
            #     #         table_select.append(
            #     #             f"{projection.table_name}.{fk.foregin_key_column} = {fk.model_table_name}.{select}"
            #     #         )
            #     #
            #     #     select_values = {}
            #     #     for _select in table_select:
            #     #         # select_values
            #     #         pass
            #
            #     # print(
            #     #     f"root {model_name} relation {fk.mapped_model_name} fields",
            #     #     names,
            #     # )
            #
            #     print(tables, table_select)
            #
            #     result = cls.objects.prefetch_related(*foregin_key_colums).all()
            #
            #     for r in result:
            #         print(model_to_dict(r))
            #
            #     s = serializer(result, many=True)
            #
            #     data.extend(s.data)

        db.close()

        return Response(data, status=status.HTTP_200_OK)
