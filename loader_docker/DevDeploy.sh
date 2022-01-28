# Dev deployment
aws ecr get-login-password --region us-east-2 | \
    docker login \
        --username AWS \
        --password-stdin 803413999848.dkr.ecr.us-east-2.amazonaws.com

docker buildx build -f DevDockerfile --platform=linux/amd64 -t pardot-agent .
docker tag pardot-agent:latest 803413999848.dkr.ecr.us-east-2.amazonaws.com/pardot-agent
docker push 803413999848.dkr.ecr.us-east-2.amazonaws.com/pardot-agent

