#!/usr/bin/env bash
set -e

for index in movies genres persons; do
    for part in index data; do
        docker run --rm -ti -v ./data:/tmp --net=async_api_sprint_1_common elasticdump/elasticsearch-dump \
            --input=/tmp/${index}_${part}.json \
            --output=http://elastic:9200/${index} \
            --type=${part}
    done
done
