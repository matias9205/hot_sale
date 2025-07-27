#!/bin/bash
set -e

echo "🚀 Ejecutando ETL_1..."
python -m scripts.etl_1

echo "📦 Ejecutando SQL..."
/opt/mssql-tools/bin/sqlcmd -S "$SQL_SERVER_HOST" -U "$SQL_SERVER_USER" -P "$SQL_SERVER_PASS" -d "$SQL_SERVER_DB" -i SQL/manage_db.sql

echo "🚀 Ejecutando ETL_2..."
python -m scripts.etl_2

echo "✅ Proceso completo."