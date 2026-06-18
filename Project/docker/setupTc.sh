#!/bin/sh

idx=$1
bandwidth=$2
latencyMap="latency"

if [ -z "$bandwidth" ]; then
  bandwidth=10000
fi

setupTC() {
  inBandwidth=$((bandwidth * 2))

  cmd="tc qdisc add dev eth0 root handle 1: htb"
  echo "$cmd"
  eval $cmd

  cmd="tc class add dev eth0 parent 1: classid 1:1 htb rate ${bandwidth}mbit"
  echo "$cmd"
  eval $cmd

  j=0
  for n in $1; do
    if [ $idx -eq $j ]; then
      j=$((j + 1))
      continue
    fi

    cmd="tc class add dev eth0 parent 1: classid 1:${j}1 htb rate ${bandwidth}mbit"
    echo "$cmd"
    eval $cmd

    cmd="tc qdisc add dev eth0 parent 1:${j}1 netem delay ${n}ms"
    echo "$cmd"
    eval $cmd

    cmd="tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst 172.10.10.${j} flowid 1:${j}1"
    echo "$cmd"
    eval $cmd
    j=$((j + 1))
  done
}

i=0
echo "Setting up tc emulated network..."
while read -r line; do
  if [ $idx -eq $i ]; then
    setupTC "$line"
    break
  fi
  i=$((i + 1))
done <"$latencyMap"

echo "Done."

/bin/sh
