FROM debian:12

RUN apt update && apt install -y \
  libpq-dev \
  postgresql-server-dev-15 \
  perl \
  cpanminus \
  libtemplate-perl \
  libdbi-perl \
  libdbd-pg-perl \
  libdancer2-perl

RUN cpanm -n Dotenv

CMD ["bash"]