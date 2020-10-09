FROM geographica/gdal2:2.4.0

ENV DEBIAN_FRONTEND=noninteractive
ENV INITRD No
ENV LANG en_US.UTF-8
ENV GOVERSION 1.15
ENV GOROOT /opt/go
ENV GOPATH /root/.go

RUN apt-get update && apt-get -y install curl
RUN curl -sL https://deb.nodesource.com/setup_14.x  | bash -

RUN apt-get update && apt-get -y install git build-essential libsqlite3-dev zlib1g-dev awscli gnupg jq nodejs unzip

RUN apt-get -y install wget && cd /opt && wget https://storage.googleapis.com/golang/go${GOVERSION}.linux-amd64.tar.gz && \
    tar zxf go${GOVERSION}.linux-amd64.tar.gz && rm go${GOVERSION}.linux-amd64.tar.gz && \
    ln -s /opt/go/bin/go /usr/bin/ && \
    mkdir $GOPATH

# Create a directory and copy in all files
RUN mkdir -p /tmp/tippecanoe-src
RUN git clone https://github.com/mapbox/tippecanoe.git /tmp/tippecanoe-src
WORKDIR /tmp/tippecanoe-src

# Build tippecanoe
RUN make && make install

# Remove the temp directory and unneeded packages
WORKDIR /
RUN rm -rf /tmp/tippecanoe-src

WORKDIR /home/app
COPY . /home/app

RUN go get github.com/muesli/clusters && go get github.com/muesli/kmeans
RUN npm install

CMD npm run worker-test 
