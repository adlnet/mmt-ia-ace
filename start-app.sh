#!/usr/bin/env bash
# start-server.sh

python manage.py waitdb 
python manage.py migrate 
python manage.py createcachetable 
python manage.py loaddata admin_theme_data.json 
python manage.py loaddata openlxp_notification_template.json 
python manage.py loaddata openlxp_notification_subject.json 
python manage.py loaddata openlxp_notification_email.json 
cd /opt/app/ 
pwd 
./start-server.sh