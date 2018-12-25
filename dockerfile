FROM rocker/rstudio:3.4.3
RUN apt-get -y update && apt-get install -y rjava
CMD /bin/bash
