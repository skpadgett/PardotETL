FROM python:3.9.6-buster

RUN mkdir /main
WORKDIR /main
ADD . /main
RUN pip install -r /main/src/requirements.txt

# RUN . /main/secrets.sh

ENV pardotEmail="segment_integrations@discoveryed.com"
ENV pardotUserKey="f5a1dc61d079e35d9a2066a4a8498c32"
ENV pardotSfConsumerKey="3MVG9IHf89I1t8hpom1l0QzbTANHH.MOASIZ3yPPhu6hFI_uipXiYi7ku85yMWg_2gXxu5bzAyWCevOHp4jrf"
ENV pardotSfConsumerSecret="BC6C2AA5D41DC0EBBBB3376CD329E92E66BD8D85DF85F9FAB9F22E86D857A29E"
ENV pardotSfRefreshToken="5Aep8618yVsldz6rZPMv4ouelrGoRNAfdaLSjo3.ILw4jdNGemJVI_MNYFoNkN9g39GUXqUEgzw3ylEVMjxAU3X"
ENV pardotSfBusinessUnitID="0Uv4P000000TNB0SAO"
ENV pardotVersion=4
# ENV AWS_ACCESS_KEY_ID="ASIA3FXKNT2YDCTWINYU"
# ENV AWS_SECRET_ACCESS_KEY="gpJbRlBIYeFOY/UPl0Gd0lURjUYUykezG/wNfH0c"
# ENV AWS_SESSION_TOKEN="IQoJb3JpZ2luX2VjEJ///////////wEaCXVzLWVhc3QtMSJHMEUCIBtsDqv71RqVfEe5l2F1SHEOqUZ67i8AGMELCmg38WSWAiEA5QM8nUNmUh07ArouUJSXLAYz749iLydxvP5ptmeYf1cqpQMIqP//////////ARAAGgw3NjgyMTcwMzAzMjAiDBXagoZsnkdJbB/0NSr5AhSnSk1BrQNVbOpN7hGU/Qqm038kPK/2npGWlAQazmiJL+F/PcDwYJC0uH/NwizQIWzZ/1UpfiKrRzFCz4t40vRgxUlUrGoC6cjdmy7/9Of/+03s73jnitlF2fahGXrmQcI0QfzVbrlCiwkYhDdgRflnON+cZFhRYWRpmt5MYJTXWX/Na2uIVjBFJdCjN8nzsXdNe4xD9hBRMw9M/uHD42FW4+eMbyN5Ghm/M/gRsNhVYWXnBEMlKyAbf3fi1eUTDJf9LnVZIBJ1y9swfH4LBzZhLa3xiP9kc3yTGNpAenEput91nAOuOKIuQyJzBD4ZXl4CwpPJUBz/1YjnUsFsflPAVsOxt45hvoMC/IESrG3GdX1wAonK8OqKMRUtgpmtXAS5ZPciVx1QPCc2qHbV48uEZ/lqUkXDGFSAbdg5a2aDwaLOPC1sUJ/S4QhjYOn7AZpvs8BQ+RMYqetmp3huMDmywXnwrWFikvGVwvnHCULGP2C3xPk4UnRSMNWgkIgGOqYBHbLywam1vZcWuCTafPzegO1h1pap+jd9aC5RyAyCXiZcmWoxP8hzkrKdsA4SwJ6Y/d2MSTrAoFIF9g5LK/T+auTb49R2piqf0vJffHsspX54k2Pb3krHAcxHTyrNq+7BOEpwlB6Yzy8p78bMQKJvrRzQuDs2eMqdP1sJbLatzsvmxrcNDkX+nrnW7MmFUpaxbHcHDXEFAMU9fINumpaLm7M6n4Vd0g=="
ENV SNOWFLAKE_ACCOUNT_IDENTIFIER="UK29315.us-east-1"
ENV SNOWFLAKE_USER="PardotEtlUser@discoveryed.com"
ENV SNOWFLAKE_PASS="p@rd0tEtlUser"
ENV AWS_DEFAULT_REGION="us-east-2"
ENV AWS_NAME_BUCKET="pardot-us-prod-20210729173046836600000001"

# CMD ["python3", "/main/src/loader.py"]
# ENTRYPOINT [ "run.sh" ]

# NOTES
# - Need a service account for AWS with access to S3 buckets to run the docker container
# - Need service account for snowflake
# - Need to confirm that using the Pardot keys in this file are appropriate
# - Need a place to run docker container (Fargate ?) and CRON job (Eventbridge ?),
#       which may mean changing/adding terraform stuff