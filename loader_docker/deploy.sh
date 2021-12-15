aws ecr get-login-password --region us-east-2 | \
    docker login \
    --username AWS \
    --password-stdin 803413999848.dkr.ecr.us-east-2.amazonaws.com
# # docker build -t etl_pardot .
#
# docker build -f Dockerfile -t etl_pardot .
# docker tag etl_pardot:latest 768217030320.dkr.ecr.us-east-1.amazonaws.com/etl_pardot:latest
# docker push 768217030320.dkr.ecr.us-east-1.amazonaws.com/etl_pardot:latest
#
# aws ecr get-login-password --region us-east-2 | \
#     docker login \
#         --username AWS \
#         --password-stdin 693047745336.dkr.ecr.us-east-2.amazonaws.com
#
# docker build -f Dockerfile -t pardot-agent .
# docker tag pardot-agent:latest 693047745336.dkr.ecr.us-east-2.amazonaws.com/pardot-agent:latest
# docker push 693047745336.dkr.ecr.us-east-2.amazonaws.com/pardot-agent:latest

aws ecr get-login-password --region us-east-2 | \
    docker login \
        --username AWS \
        --password-stdin 693047745336.dkr.ecr.us-east-2.amazonaws.com

docker build -f Dockerfile -t pardot-agent .
docker tag pardot-agent:latest 803413999848.dkr.ecr.us-east-2.amazonaws.com/pardot-agent
docker push 803413999848.dkr.ecr.us-east-2.amazonaws.com/pardot-agent
