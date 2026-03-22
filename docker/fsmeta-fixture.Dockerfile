FROM ubuntu:24.04

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libgcc-s1 libc6 libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY capanix-benchmark-run/docker-stage/fsmeta-bin/fs_meta_api_fixture /usr/local/bin/fs_meta_api_fixture
COPY docker/fsmeta-fixture-entrypoint.sh /usr/local/bin/fsmeta-fixture-entrypoint.sh

RUN chmod +x /usr/local/bin/fs_meta_api_fixture /usr/local/bin/fsmeta-fixture-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/fsmeta-fixture-entrypoint.sh"]
