export nifi_host="localhost"
export nifi_port="8443"
export clientId="b9112890-f366-44d4-a0d3-6ed7f4d53cec"
export pg_id="6f8f46ad-5017-32f9-8d71-b68fca956eb3"

# Get jwt authorization token
export jwt=$(curl -X POST https://$nifi_host:$nifi_port/nifi-api/access/token -k \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB")

# Get root process group
export root_pg=$(curl -X GET "https://$nifi_host:$nifi_port/nifi-api/process-groups/root" -k \
  -H "Authorization: Bearer $jwt" | jq -r ".id")

# Set process group status (RUNNING or STOPPED)
curl -X PUT "https://$nifi_host:$nifi_port/nifi-api/flow/process-groups/$root_pg" -k \
  -H "content-type: application/json" \
  -H "Authorization: Bearer $jwt" \
  -d "{\"id\":\"$root_pg\",\"disconnectedNodeAcknowledged\":false,\"state\":\"RUNNING\"}"

# Get running processors in the root process group
export status=$(curl -X GET "https://$nifi_host:$nifi_port/nifi-api/flow/process-groups/$root_pg/status?recursive=true" -k\
  -H "Content-type: application/json" \
  -H "Authorization: Bearer $jwt" | jq '
      .processGroupStatus
      .aggregateSnapshot
      .processGroupStatusSnapshots[0]
      .processGroupStatusSnapshot
      .processorStatusSnapshots
      | map(select(.processorStatusSnapshot.runStatus=="Running"))
      | length
    '
  )

# There are a total of 10 processors in the process group use, so:
case "$status" in
  10) echo "Running" ;; # if status = 10, then the processor group is running
  0)  echo "Stopped" ;; # if status = 0,  then the processor group is stopped
  *)  echo "Error: unexpected processors running ($status)" ;; # otherwise something went wrong
esac
