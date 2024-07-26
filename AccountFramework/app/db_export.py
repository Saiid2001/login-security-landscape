from abc import ABC, abstractmethod
import argparse
import datetime
import json
import pathlib
from typing import Dict, List, Optional
import jsonschema
import db


TYPE_MAP = {
    "IntegerField": "number",
    "DateTimeField": "string",
    "DateField": "string",
    "CharField": "string",
    "TextField": "string",
    "BooleanField": "boolean",
    "JsonField": "object",
}

DEFAULTS_MAP = {
    datetime.datetime.now: "NOW()",
}


def generate_model_schema(table: db.Model, columns: Optional[List[str]] = None):

    if columns is None:
        columns = list(table._meta.columns.keys())

    table_name = table._meta.table_name
    class_name = table.__name__

    cols = dict()

    for key, value in table._meta.columns.items():
        if key != "id" and key in columns:
            cols[key] = value

    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": f"{table_name} ({class_name})",
        "description": table.__doc__.replace("\n", " "),
        "type": "object",
        "properties": dict(),
        "required": list(),
    }

    for key, value in cols.items():
        typ = type(value).__name__
        if typ == "ForeignKeyField" or typ == "DeferredForeignKey":
            schema["properties"][key] = {
                "type": ["string", "null", "number"],
                "description": value.help_text.replace("\n", " "),
            }

        else:
            schema["properties"][key] = {
                "type": TYPE_MAP[typ],
                "description": value.help_text.replace("\n", " "),
            }

        if value.default is not None:
            schema["properties"][key]["default"] = DEFAULTS_MAP.get(
                value.default, value.default
            )

        if value.null is False:
            schema["required"].append(key)

    return schema


def combine_schemas(title, schemas: Dict[str, dict]):
    combined = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": title,
        "type": "object",
        "properties": dict(),
        "required": list(),
    }

    # each schema should become a list of such objects
    """
    {
        "schema1 key" : [ { schema1}, {schema1}, ...],
        "schema2 key" : [ { schema2}, {schema2}, ...],
        ...
    }
    """

    for key, value in schemas.items():
        combined["properties"][key] = {
            "type": "array",
            "items": value,
        }
        combined["required"].append(key)

    return combined


EXPORT_TYPES: Dict[str, "Export"] = dict()


class Export(ABC):
    id: str

    @staticmethod
    @abstractmethod
    def validate(data: dict) -> bool:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def export(file: str):
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def load(file: str):
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def generate_schema():
        raise NotImplementedError()

    # add  the id on creation of subclasses
    def __init_subclass__(cls, **kwargs):
        EXPORT_TYPES[cls.id] = cls


__dir__ = pathlib.Path(__file__).parent

SCHEMAS_DIR = __dir__ / "schemas"

SCHEMAS_DIR.mkdir(exist_ok=True)


class SitesAndAccountsExport(Export):
    id = "sites_and_accounts"

    @staticmethod
    def validate(data: dict):
        pass

    @staticmethod
    def export(file: str):
        pass

    @staticmethod
    def load(file: str):
        pass

    @staticmethod
    def generate_schema():

        sites_schema = generate_model_schema(
            db.Website,
            columns=[
                "origin",
                "site",
                "landing_page",
                "t_rank",
                "c_bucket",
                "tranco_date",
                "crux_date",
            ],
        )
        accounts_schema = generate_model_schema(
            db.Account,
            columns=[
                "website",
                "credential",
                "registration_result",
                "account_status",
                "registration_note",
                "login_note",
                "validation_note",
            ],
        )

        credentials_schema = generate_model_schema(
            db.Credentials,
            columns=[
                "username",
                "password",
                "email",
                "identity",
                "website",
            ],
        )

        identities_schema = generate_model_schema(
            db.Identity,
        )

        schema = combine_schemas(
            "Sites and Accounts",
            {
                "websites": sites_schema,
                "accounts": accounts_schema,
                "credentials": credentials_schema,
                "identities": identities_schema,
            },
        )

        jsonschema.Draft7Validator.check_schema(schema)

        return schema


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="JSON exports of the database",
    )

    subparsers = parser.add_subparsers(dest="command")

    # Generate schemas

    gen_schema_parser = subparsers.add_parser(
        "generate-schemas", help="Generate schemas for the database"
    )

    # Export

    export_parser = subparsers.add_parser("export", help="Export the database")

    export_parser.add_argument("file", help="File to export to")

    export_parser.add_argument(
        "--type",
        help="Type of export",
        choices=EXPORT_TYPES.keys(),
        default="sites_and_accounts",
    )

    args = parser.parse_args()

    if args.command == "generate-schemas":

        for export_type in EXPORT_TYPES.values():
            schema = export_type.generate_schema()
            with open(SCHEMAS_DIR / f"{export_type.id}.json", "w") as f:
                json.dump(schema, f, indent=4)

    elif args.command == "export":

        db.initialize_db()

        if args.type not in EXPORT_TYPES:
            raise ValueError(f"Unknown export type: {args.type}")

        EXPORT_TYPES[args.type].export(args.file)
