FROM python:3.9.1-buster

RUN mkdir /main
WORKDIR /main
ADD . /main
RUN pip install -r /main/src/requirements.txt


EXPOSE 8000
CMD ["python3", "/main/src/main.py"]