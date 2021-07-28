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
ENV AWS_ACCESS_KEY_ID="ASIA3FXKNT2YKD66BYYL"
ENV AWS_SECRET_ACCESS_KEY="ZpuX0WWMI9dKfL8AmMi61FK4dB3Pf2xGFJ86R6LG"
ENV SNOWFLAKE_ACCOUNT_IDENTIFIER="UK29315.us-east-1"
ENV SNOWFLAKE_USER="PardotEtlUser@discoveryed.com"
ENV SNOWFLAKE_PASS="p@rd0tEtlUser"

CMD ["python3", "/main/src/loader.py"]
# ENTRYPOINT [ "run.sh" ]

# NOTES
# - Need a service account for AWS with access to S3 buckets to run the docker container
# - Need service account for snowflake
# - Need to confirm that using the Pardot keys in this file are appropriate
# - Need a place to run docker container (Fargate ?) and CRON job (Eventbridge ?),
#       which may mean changing/adding terraform stuff