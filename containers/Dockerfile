FROM arangodb:3.7
MAINTAINER Frank Celler <info@arangodb.com>

ADD debug-scripts.tar.gz /
ADD arangodb-debug.sh /

ENTRYPOINT [ "/arangodb-debug.sh" ]
