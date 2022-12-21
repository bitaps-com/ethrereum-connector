docker run -d \
	--name test-connector-postgres \
	-p 127.0.0.1:5432:5432 \
	-v $(pwd)/docker-entrypoint-initdb.d/:/docker-entrypoint-initdb.d/  \
	-v $(pwd)/../data/postgres/:/var/lib/postgresql/data/  \
	postgres:11