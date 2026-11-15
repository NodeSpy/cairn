#!/bin/bash
# usage: run.sh N [stages]   (run on lfs-srv as daniel, uses sudo)
N=$1; STAGES=${2:-gen scan marks leases fid}
B=/cache/exp-scale/scale; T=/cache/exp-scale/tree-$N; R=/cache/exp-scale/results/$N; mkdir -p $R
S="sudo $B"
for st in $STAGES; do
 case $st in
 gen) sudo rm -rf $T; sync
   $S gen $T $N 2 | tee $R/gen.log
   sudo btrfs filesystem df /cache | tee $R/btrfs_df.log; sudo btrfs filesystem usage -b /cache >> $R/btrfs_df.log; df -h /cache | tee -a $R/btrfs_df.log;;
 scan) $S scan $T | tee $R/scan.log;;
 marks)
   [ $N = 100000 ] && $S marks $T inode limited | tee $R/marks_limited.log
   for m in sb dir inode; do $S marks $T $m | tee $R/marks_$m.log; done;;
 leases) $S leases $T $N 200000 | tee $R/leases.log;;
 fid) $S fid $T dirs fast unlimited | tee $R/fid_dirs_fast.log
   $S fid $T sb fast unlimited | tee $R/fid_sb_fast.log
   $S fid $T dirs slow unlimited | tee $R/fid_dirs_slow_unl.log
   $S fid $T dirs slow limited | tee $R/fid_dirs_slow_lim.log;;
 esac
done
