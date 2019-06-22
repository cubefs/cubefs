#!/bin/sh

host=127.0.0.1
port=3000

user=${GRAFANA_USERNAME:-admin}
pass=${GRAFANA_PASSWORD:-123456}
url="http://${user}:${pass}@${host}:${port}"
url2="http://${host}:${port}"
ds_url="${url2}/api/datasources"
dashboard_url="${url2}/api/dashboard/db"

org="chubaofs.io"

parse_json(){
    echo "$1" | sed "s/.*\"$2\":\([^,}]*\).*/\1/"
    #echo "${1//\"/}" | sed "s/.*$2:\([^,}]*\).*/\1/"
}

# add org
req=$(curl -s -X POST -H "Content-Type: application/json" -d '{"name":"${org}"}' ${url}/api/orgs)
echo ${req}

message=$(parse_json "${req}" "message" )

echo "$message"

orgid=$(parse_json "${req}" "orgId")
# set org to admin
curl -s -X POST ${url}/api/user/using/${orgid}

# get token key
#curl -s -X POST -H "Content-Type: application/json" -d '{"name":"apikeycurl", "role": "Admin"}' ${url}/api/auth/keys
rep=$(curl -s -X POST -H "Content-Type: application/json" -d '{"name":"apikeycurl", "role": "Admin"}' ${url}/api/auth/keys)
echo ${req}
key=$(parse_json "${rep}" "key")
echo "key ${key}"

echo "add ds"
# add datasource
curl -s -X POST -H "Content-Type: application/json" -u ${user}:${pass} ${ds_url} -d @/grafana/ds.json
#until curl -s -XPOST -H "Content-Type: application/json" --connect-timeout 1 -u ${userName}:${password} ${ds_url} -d @/grafana/ds.json >/dev/null; do
    #sleep 1
#done

# add dashboard

#echo "add dashboard"
curl  -X POST --insecure -H "Authorization: Bearer ${key}" -H "Content-Type: application/json" -d @/grafana/dashboard.json ${url2}/api/dashboards/db
