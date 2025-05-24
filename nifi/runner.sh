export nifi_host="localhost"
export nifi_port="8443"
export clientId="b9112890-f366-44d4-a0d3-6ed7f4d53cec"
export pg_id="6f8f46ad-5017-32f9-8d71-b68fca956eb3"

export jwt=$(curl -X POST https://$nifi_host:$nifi_port/nifi-api/access/token -k \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB")

export root_pg=$(curl -X GET "https://$nifi_host:$nifi_port/nifi-api/process-groups/root" -k \
  -H "Authorization: Bearer $jwt" | jq -r ".id")

curl -X PUT "https://$nifi_host:$nifi_port/nifi-api/flow/process-groups/$root_pg" -k \
  -H "content-type: application/json" \
  -H "Authorization: Bearer $jwt" \
  -d "{\"id\":\"$root_pg\",\"disconnectedNodeAcknowledged\":false,\"state\":\"RUNNING\"}"
#  -d "{\"id\":\"$root_pg\",\"disconnectedNodeAcknowledged\":false,\"state\":\"RUNNING\"}"