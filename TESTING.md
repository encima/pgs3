## How to test

1. Start a SIMDU on a project
2. List the volumes for the source instance: `aws ec2 describe-instances --profile supa-dev --region eu-central-1 --instance-ids X`
3. Get the data volume ID
4. Copy the volume: ` aws ec2 copy-volumes --source-volume-id vol-0b7cbbdb6d1b61b42 --profile supa-dev --region eu-central-1`
5. Get the copied volume ID
6. Find the data volume for the target instance with the same command as 2
7. Detach that volume: `aws ec2 detach-volume --volume-id i-0adb4cb7893e14a94 --instance-id i-0adb4cb7893e14a94 --region eu-central-1`
8. Attach the copied volume: ` aws ec2 attach-volume --volume-id vol-0a3ce399b084cf818 --instance-id i-0adb4cb7893e14a94 --device /dev/xvdh --profile supa-dev --region eu-central-1`
9. SSH to the instance, ensure it is mounted: `umount /data && mount /dev/nvme2n1 /data`
10. Get the LSN from the shutdown: `pg_controldata /data/pgdata | grep location`
