FROM geographica/gdal2:2.4.0

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get -y install curl
RUN curl -sL https://deb.nodesource.com/setup_10.x  | bash -

RUN apt-get update && apt-get -y install git build-essential libsqlite3-dev zlib1g-dev awscli gnupg jq nodejs

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

RUN npm install

CMD npm run worker-local 
