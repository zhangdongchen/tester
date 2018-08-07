From rocker/tidyverse

# Edit sources list
COPY ./sources.list /etc/apt/

RUN apt-get update -qq && apt-get install -y \
    --allow-unauthenticated \
    --allow-downgrades \ 
    git-core \
    libssl-dev \
    libcurl3-gnutls=7.47.0-1ubuntu2.8 \
    libcurl4-gnutls-dev \
    # for Mongolite
    libsasl2-2=2.1.26.dfsg1-14build1 \
    libsasl2-dev \
    # for DBI
    libjpeg62-dev
  
RUN install2.r --error \
  # change to China mirror
  -r "http://mirrors.tuna.tsinghua.edu.cn/CRAN/" \
  dbplyr \
  jsonlite \
  DBI \
  mongolite \
  yaml \
  plumber \
  httr \
  cli
  

# Set working directory
WORKDIR /tester

# Add scripts and stuffs
ADD . /tester

## Add the wait script to the image
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.3.0/wait /wait
RUN chmod +x /wait && /wait

# Open port
EXPOSE 6666

# Activate api service when boot up
# CMD ["Rscript", "--vanilla", "6666_fengkong_accurate_evaluation.R", "&"]


