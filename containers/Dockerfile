FROM arangodb:3.7
MAINTAINER Frank Celler <info@arangodb.com>

ADD maintenance.tar.gz /
ADD maintenance.sh /

ENTRYPOINT [ "/maintenance.sh" ]
