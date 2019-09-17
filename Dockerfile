FROM python:3.7.0

ADD *.py /
ADD requirements.txt /
RUN pip install -r requirements.txt

USER root
VOLUME [ "/conf", "/certs", "/root/.ssh", "/logs" ]
CMD [ "python", "./.py", "-c", "/conf/configuration.ini"]