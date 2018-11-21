# Use an official Python runtime as a parent image
#FROM ubuntu:14.04
FROM python:3.4

# Set the working directory to /data-interpreter-tool-back-end
WORKDIR /dit

#Clone repo back-end
RUN apt-get update && apt-get install -y git
RUN apt-get install net-tools -y

RUN git clone https://miquel_atos@bitbucket.org/cityenabler/data-interpreter-tool-back-end.git

#install db sqlite3
RUN apt-get install sqlite3 -y

#install db  - postgresql
#RUN apt-get install -y postgresql
#RUN pip install psycopg2==2.5.4
#RUN sudo -u postgres createuser pcompass -W
#RUN sudo -u postgres createdb pcompass --owner pcompass


# Install any needed packages specified in requirements.txt


WORKDIR /dit/data-interpreter-tool-back-end

COPY requirements2.txt requirements2.txt
COPY test.py test.py

RUN pip install -r requirements.txt
RUN pip install -r requirements2.txt

RUN cp config/settings.sample.py config/settings.py


RUN python manage.py migrate
RUN echo "from django.contrib.auth import get_user_model; User = get_user_model(); User.objects.create_superuser('adminCedus', 'admin@cedus.com', 'passwordC3du5')" | python manage.py shell


RUN python manage.py syncdb
RUN python manage.py loaddata metrics events references visualizations indicators ags

# Make port 8000 available to the world outside this container
EXPOSE 8000


CMD chmod +x manage.py
# Run Back end when the container launches
CMD [ "python", "./manage.py", "runserver", "0.0.0.0:8000" ]

#CMD [ "python", "./test.py" ]
