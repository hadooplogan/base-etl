#!/bin/bash
#执行es索引创建脚本
echo "start create ent-contact-extract"

curl -XPUT 'http://192.168.207.14:59200/ent-contact-extract

' -d '{
"settings": {
"index": {
"number_of_shards": "5",
"number_of_replicas": "1",
"refresh_interval": "30s",
"codec": "best_compression",
"translog.sync_interval": "5s"
}
},
"mappings": {
"ENT-CONTACT-EXTRACT": {
"properties": {
"pripid": {
"type": "keyword",
"index": true
}
,
"credit_code": {
"type": "keyword",
"index": true
},
"entname": {
"type": "keyword",
"index": true
},
"tel": {
"type": "keyword",
"index": true
},
"email": {
"type": "keyword",
"index": true
},
"address": {
"type": "keyword",
"index": true
},
"date":{
"type": "keyword",
"index": true
}
}
}
}
}'
echo "it's all good"

echo "start create ent_contactinfo_full"

curl -XPUT 'http://192.168.207.14:59200/ent_contactinfo_full

' -d '{
"settings": {
"index": {
"number_of_shards": "5",
"number_of_replicas": "1",
"refresh_interval": "30s",
"codec": "best_compression",
"translog.sync_interval": "5s"
}
},
"mappings": {
"ENT_CONTACTINFO_FULL": {
"properties": {
"id": {
"type": "keyword",
"index": true
},
"pripid": {
"type": "keyword",
"index": true
}
,
"credit_code": {
"type": "keyword",
"index": true
},
"entname": {
"type": "keyword",
"index": true
},
"tel": {
"type": "keyword",
"index": true
},
"email": {
"type": "keyword",
"index": true
},
"address": {
"type": "keyword",
"index": true
},
"position": {
"type": "keyword",
"index": true
},
"source": {
"type": "keyword",
"index": true
},
"date":{
"type": "keyword",
"index": true
}
}
}
}
}'
echo "it's all good!"