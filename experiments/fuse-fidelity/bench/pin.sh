#!/bin/bash
# pin.sh: effect of CPU placement on request latency
sudo mkdir -p /mnt/ram/pin; sudo chown daniel: /mnt/ram/pin
cat /sys/module/fuse/parameters/enable_uring 2>/dev/null
mnt() { sudo sh -c "$1 /tmp/fuse-pt --backing /mnt/ram/pin --mount /mnt/lfs --passthrough $2 >/dev/null 2>&1 &"; sleep 0.5; }
um() { sudo umount /mnt/lfs; sleep 0.5; }
mnt "" ""; mkdir /mnt/lfs/d; for i in $(seq 1 3000); do : > /mnt/lfs/d/f$i; done; um
ls /mnt/ram/pin/d | sed 's|^|/mnt/lfs/d/|' | sort > /tmp/pin.list
run() { # run <label> <daemon prefix> <daemon flags> <lat prefix>
  mnt "$2" "$3"; echo 2 | sudo tee /proc/sys/vm/drop_caches >/dev/null
  for ph in stat open_cold; do
    echo -n "$1 $ph: "; $4 /tmp/lat $ph /tmp/pin.list 3000
    echo 2 | sudo tee /proc/sys/vm/drop_caches >/dev/null
  done
  um
}
run "unpinned" "" "" ""
run "unpinned thr1" "" "--threads 1" ""
run "daemon cpu1, app cpu0, thr1" "taskset -c 1" "--threads 1" "taskset -c 0"
run "daemon cpu0, app cpu0, thr1" "taskset -c 0" "--threads 1" "taskset -c 0"
run "daemon cpu0, app cpu0, thr4" "taskset -c 0" "" "taskset -c 0"
run "unpinned again" "" "" ""
sudo rm -rf /mnt/ram/pin
