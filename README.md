TODO - make this actually good

You'll probably need to run
chmod +x init-airflow.sh

And then run
docker-compose down
docker-compose up --build


or you can use make commands tbc need to do this properly.....

you should be able to login with
airflow and airflow as username and password

but this wasnt working for me so I did
docker-compose exec airflow-webserver bash

then from container bash shell
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com