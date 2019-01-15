FROM ubuntu:trusty

RUN apt-get update && apt-get install -y apache2 \
    libapache2-mod-wsgi \
    build-essential \
        vim \
 && apt-get clean \
 && apt-get autoremove \
 && rm -rf /var/lib/apt/lists/*

# Install packages for python
RUN apt-get update && apt-get install -yq --no-install-recommends \
    python python-dev python-pip \
    python-setuptools pkg-config \
    libfreetype6-dev libxml2-dev libxslt1-dev \
    libpng12-dev zlib1g-dev libjpeg-dev \
    gcc g++ \
    python-pil python-tk \
    python-numpy python-scipy python-pandas \
    python-requests \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install dax
RUN pip install pandas==0.23.4
RUN pip install nibabel==2.3.0
RUN pip install matplotlib==2.2.2
RUN pip install dax
RUN pip install https://github.com/VUIIS/dax/archive/baxter.zip

# Install DASH stuff
RUN pip install dash dash-core-components==0.21.0rc1 dash-html-components \
    dash-renderer dash-table-experiments plotly xlrd

# Copy over files
COPY ./deploy/dashboard.conf /etc/apache2/sites-available/dashboard.conf
COPY ./deploy/dashboard.wsgi /var/www/dashboard/dashboard.wsgi
COPY ./deploy/run.py /var/www/dashboard/run.py
COPY ./deploy/config.yaml /var/www/dashboard/config.yaml
COPY ./dashboard /var/www/dashboard/dashboard/

# Configure apache, not sure what this actually does
RUN a2ensite dashboard
RUN a2enmod headers
RUN a2dissite 000-default.conf
RUN a2ensite dashboard.conf

EXPOSE 80

WORKDIR /var/www/dashboard

# Copy data
COPY ./DATA /var/www/dashboard/DATA/

CMD  /usr/sbin/apache2ctl -D FOREGROUND
