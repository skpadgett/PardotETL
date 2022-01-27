# Dev deployment
aws ecr get-login-password --region us-east-2 | \
    docker login \
        --username AWS \
        --password-stdin 693047745336.dkr.ecr.us-east-2.amazonaws.com

docker buildx build -f DevDockerfile --platform=linux/amd64 -t pardot-agent .
docker tag pardot-agent:latest 693047745336.dkr.ecr.us-east-2.amazonaws.com/pardot-agent
docker push 693047745336.dkr.ecr.us-east-2.amazonaws.com/pardot-agent