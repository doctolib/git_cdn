FROM python:3.10-alpine

WORKDIR     /app

# Only install dependencies
RUN  apk --no-cache add \
        git \
        libstdc++ \
        make \
     && \
     apk add --update --no-cache \
        curl \
        gzip \
        libffi \
        openssl

ADD dist/git_cdn-*.whl requirements.txt /app/
RUN apk add --update --no-cache --virtual \
        .build-deps \
        alpine-sdk \
        libffi-dev \
        musl-dev \
        openssl-dev \
    &&\
    python -m pip install --constraint requirements.txt /app/git_cdn-*.whl && \
    apk del .build-deps

# Configure git for git-cdn
RUN git config --global pack.threads 4  &&\
    git config --global http.postBuffer 524288000 && \
    # Allow git clone/fetch --filter
    git config --global uploadpack.allowfilter true

ADD config.py /app/

# entrypoint contains stuff that you shouldn't want to customize
# starts gunicorn
ENTRYPOINT ["gunicorn", "git_cdn.app:app", "-c", "config.py"]
# CMD holds the optional arguments (change at will)
CMD ["--bind", ":8000"]
EXPOSE 8000
