from decouple import config

DATABASE_CONFIG={
"DRIVER": "ODBC Driver 17 for SQL Server",
"SERVER" : config("MSSQL_HOST"),
"DSN" : "sigadw",
"Description" : "producao",
"DATABASE" : config("MSSQL_DATABASE"),
"UID" : config("MSSQL_USER"),
"PWD": config("MSSQL_PASS")
}