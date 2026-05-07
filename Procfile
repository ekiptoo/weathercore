release: mysql -h $MYSQLHOST -u $MYSQLUSER -p$MYSQLPASSWORD -P $MYSQLPORT $MYSQLDATABASE < schema.sql
web: gunicorn app:app --workers 1 --timeout 120