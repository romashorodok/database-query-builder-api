import re
import keyword

from http import HTTPMethod
from typing import Any

from django.db import models
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.utils import load_backend
from rest_framework import status
from rest_framework.decorators import action
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

            for table_name in sorted(name for name in table_info):
                try:
                    relations = db.introspection.get_relations(cursor, table_name)
                except NotImplementedError:
                    relations = {}

                primary_key_columns = db.introspection.get_primary_key_columns(
                    cursor, table_name
                )
                primary_key_column = (
                    primary_key_columns[0] if primary_key_columns else None
                )

                try:
                    constraints = db.introspection.get_constraints(cursor, table_name)
                except NotImplementedError:
                    constraints = {}
                unique_columns = [
                    c["columns"][0]
                    for c in constraints.values()
                    if c["unique"] and len(c["columns"]) == 1
                ]

                table_description = db.introspection.get_table_description(
                    cursor, table_name
                )

                model_name = normalize_table_name(table_name)
                print("class %s(models.Model):" % model_name)

                known_models = [model_name]
                used_column_names = []  # Holds column names used in the table so far
                column_to_field_name = {}  # Maps column names to names of model fields
                used_relations = set()  # Holds foreign relations used in the table.

                for row in table_description:
                    comment_notes = []  # Holds Field notes, to be displayed in a Python comment.
                    extra_params = {}  # Holds Field parameters such as 'db_column'.
                    column_name = row.name
                    is_relation = column_name in relations

                    att_name, params, notes = normalize_col_name(
                        column_name, used_column_names, is_relation
                    )
                    extra_params.update(params)
                    comment_notes.extend(notes)

                    used_column_names.append(att_name)
                    column_to_field_name[column_name] = att_name

                    if column_name == primary_key_column:
                        extra_params["primary_key"] = True
                        if primary_key_columns and len(primary_key_columns) > 1:
                            comment_notes.append(
                                "The composite primary key (%s) found, that is not "
                                "supported. The first column is selected."
                                % ", ".join(primary_key_columns)
                            )
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
                            ref_pk_column = db.introspection.get_primary_key_column(
                                cursor, ref_db_table
                            )
                            if ref_pk_column and ref_pk_column != ref_db_column:
                                extra_params["to_field"] = ref_db_column
                        rel_to = (
                            "self"
                            if ref_db_table == table_name
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
                    else:
                        # Calling `get_field_type` to get the field type string and any
                        # additional parameters and notes.
                        field_type, field_params, field_notes = get_field_type(
                            db, table_name, row
                        )
                        extra_params.update(field_params)
                        comment_notes.extend(field_notes)

                        field_type += "("

                    if att_name == "id" and extra_params == {"primary_key": True}:
                        if field_type == "AutoField(":
                            continue
                        elif (
                            field_type
                            == db.features.introspected_field_types["AutoField"] + "("
                        ):
                            comment_notes.append("AutoField?")

                    # Add 'null' and 'blank', if the 'null_ok' flag was present in the
                    # table description.
                    if row.null_ok:  # If it's NULL...
                        extra_params["blank"] = True
                        extra_params["null"] = True

                    field_desc = "%s = %s%s" % (
                        att_name,
                        # Custom fields will have a dotted path
                        "" if "." in field_type else "models.",
                        field_type,
                    )
                    if field_type.startswith(("ForeignKey(", "OneToOneField(")):
                        field_desc += ", models.DO_NOTHING"

                    if db.features.supports_comments and row.comment:
                        extra_params["db_comment"] = row.comment

                    if extra_params:
                        if not field_desc.endswith("("):
                            field_desc += ", "
                        field_desc += ", ".join(
                            "%s=%r" % (k, v) for k, v in extra_params.items()
                        )
                    field_desc += ")"
                    if comment_notes:
                        field_desc += "  # " + " ".join(comment_notes)

                    print("field desc:", "    %s" % field_desc)

                    comment = None

                    if info := table_info.get(table_name):
                        is_view = info.type == "v"
                        is_partition = info.type == "p"
                        if db.features.supports_comments:
                            comment = info.comment
                    else:
                        is_view = False
                        is_partition = False

                    meta = get_meta(
                        table_name,
                        constraints,
                        column_to_field_name,
                        is_view,
                        is_partition,
                        comment,
                    )
                    # print(meta)

                    fields = {}

                    # Add some default fields if necessary
                    fields["id"] = models.AutoField(primary_key=True)

                    # Define the Meta class if needed (this is optional)
                    if meta:

                        class Meta:
                            db_table = table_name
                            managed = False
                            app_label = ""

                        fields["Meta"] = Meta

                    fields["__module__"] = ""

                    django_model_class = type(
                        model_name,
                        (models.Model,),
                        fields,
                    )
                    model: models.Model = django_model_class
                    print("model class", model)

                    dynamic_result = model.objects.all()
                    print(
                        "result from",
                        django_model_class,
                        dynamic_result,
                    )

        db.close()

        data = [
            {"id": 1, "name": "Item 1", "description": "Description 1"},
            {"id": 2, "name": "Item 2", "description": "Description 2"},
        ]
        return Response(data, status=status.HTTP_200_OK)
