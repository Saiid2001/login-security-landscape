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
        if key in columns:
            cols[key] = value

        if key[-3:] == "_id" and key[:-3] in columns:
            cols[key[:-3]] = value

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

        elif typ == "AutoField":
            schema["properties"][key] = {
                "type": "number",
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
        else:

            typ = schema["properties"][key]["type"]
            if isinstance(typ, list):
                if "null" not in typ:
                    schema["properties"][key]["type"].append("null")
            else:
                schema["properties"][key]["type"] = [typ, "null"]

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


def to_dicts(query, columns: Optional[List[str]] = None):

    dicts = query.dicts()

    # turn datetime into iso string recurisvely
    def convert_datetime(obj):
        _new_obj = dict()
        for key, value in obj.items():
            if isinstance(value, datetime.datetime) or isinstance(value, datetime.date):
                _new_obj[key] = value.isoformat()
            elif isinstance(value, dict):
                _new_obj[key] = convert_datetime(value)
            else:
                _new_obj[key] = value
        return _new_obj

    dicts = [convert_datetime(obj) for obj in dicts]

    if columns is not None:
        return [
            {
                key: value
                for key, value in obj_dict.items()
                if key in columns or (key[:-3] in columns and key[-3:] == "_id")
            }
            for obj_dict in dicts
        ]

    return dicts


EXPORT_TYPES: Dict[str, "Export"] = dict()


class Export(ABC):
    id: str

    @classmethod
    def validate(cls, data: dict):
        schema = EXPORT_TYPES[cls.id].generate_schema()
        jsonschema.validate(data, schema)

    @staticmethod
    @abstractmethod
    def export(file: str):
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def load(data: dict):
        raise NotImplementedError()

    @classmethod
    def load_from_file(cls, file: str):

        file = pathlib.Path(file)
        if not file.exists():
            raise FileNotFoundError(f"File {file} does not exist")

        if not file.is_file() or not file.suffix == ".json":
            raise ValueError(f"File {file} is not a json file")

        with open(file, "r") as f:
            data = json.load(f)
            cls.validate(data)
            out = cls.load(data)
            
        return out

    @classmethod
    def load_from_json(cls, json_data: str):
        data = json.loads(json_data)
        cls.validate(data)
        out = cls.load(data)
        return out

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


class TimelessExport(Export):
    id = "timeless"

    # columns to include
    websites_columns = [
        "id",
        "origin",
        "site",
        "landing_page",
        "t_rank",
        "c_bucket",
        "tranco_date",
        "crux_date",
    ]

    accounts_columns = [
        "website",
        "credentials",
        "registration_result",
        "account_status",
        "registration_note",
        "login_note",
        "validation_note",
    ]

    credentials_columns = [
        "id",
        "username",
        "password",
        "email",
        "identity",
        "website",
    ]

    identities_columns = [
        "id",
        "username",
        "email",
        "password",
        "first_name",
        "last_name",
        "gender",
        "country",
        "zip_code",
        "city",
        "address",
        "birthday",
        "phone",
        "storage_json",
    ]

    @staticmethod
    def export(file: str):

        db.initialize_db()

        sites = db.Website.select()
        accounts = db.Account.select()
        credentials = db.Credentials.select()
        identities = db.Identity.select()

        data = {
            "websites": to_dicts(sites, TimelessExport.websites_columns),
            "accounts": to_dicts(accounts, TimelessExport.accounts_columns),
            "credentials": to_dicts(credentials, TimelessExport.credentials_columns),
            "identities": to_dicts(identities, TimelessExport.identities_columns),
        }

        jsonschema.validate(data, TimelessExport.generate_schema())

        with open(file, "w") as f:
            json.dump(data, f, indent=4)

    @staticmethod
    def load(data: dict) -> List[int]:
    

        jsonschema.validate(data, TimelessExport.generate_schema())

        # First load the identities and store the mapping of id to identity
        # id_map = {id_in_file: id_in_db}
        identity_id_map = {}

        with db.db.transaction() as transaction:
            
            try:
                for identity_obj in data["identities"]:

                    # error if an identity with the same email already exists
                    # if (
                    #     db.Identity.select()
                    #     .where(db.Identity.email == identity_obj["email"])
                    #     .exists()
                    # ):
                    #     raise ValueError(
                    #         f"Identity with email {identity_obj['email']} already exists"
                    #     )
                    

                    _id = identity_obj.pop("id")
                    identity, _ = db.Identity.get_or_create(**identity_obj)
                    identity_id_map[_id] = identity.id

                # load websites or get them
                website_id_map = {}

                for website_obj in data["websites"]:
                    _id = website_obj.pop("id")

                    if (
                        db.Website.select()
                        .where(db.Website.site == website_obj["site"])
                        .exists()
                    ):
                        website = db.Website.get(site=website_obj["site"])
                    else:

                        website = db.Website.create(**website_obj)
                    website_id_map[_id] = website.id

                # load credentials after mapping ids for them
                credentials_id_map = {}

                for credential_obj in data["credentials"]:
                    _id = credential_obj.pop("id")
                    credential_obj["identity"] = identity_id_map[credential_obj["identity"]]
                    credential_obj["website"] = website_id_map[credential_obj["website"]]
                    credentials = db.Credentials.create(**credential_obj)
                    credentials_id_map[_id] = credentials.id

                # load accounts
                for account_obj in data["accounts"]:
                    account_obj["website"] = website_id_map[account_obj["website"]]
                    account_obj["credentials"] = credentials_id_map[account_obj["credentials"]]
                    db.Account.create(**account_obj)
                    
            except Exception as e:
                transaction.rollback()
                raise e
            
            return list(credentials_id_map.values())

    @staticmethod
    def generate_schema():

        websites_schema = generate_model_schema(
            db.Website, columns=TimelessExport.websites_columns
        )

        accounts_schema = generate_model_schema(
            db.Account,
            columns=TimelessExport.accounts_columns,
        )

        credentials_schema = generate_model_schema(
            db.Credentials, columns=TimelessExport.credentials_columns
        )

        identities_schema = generate_model_schema(
            db.Identity, columns=TimelessExport.identities_columns
        )

        schema = combine_schemas(
            "Sites and Accounts",
            {
                "websites": websites_schema,
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
        default="timeless",
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
