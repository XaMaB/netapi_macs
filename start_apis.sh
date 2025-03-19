

PYTHONPATH=$PYTHONPATH:/root/API_routers

/usr/bin/gunicorn -t 1200 --workers 4 ses_in_mongo_v4:app -b 127.0.0.1:12000 --log-file export_sessions.log --error-logfile export_sessions_error.log &
/usr/bin/gunicorn -t 1200 --workers 4 index_to_csv_v3:app -b 127.0.0.1:13000 --log-file client_mac.log --error-logfile client_mac_error.log &


