export VOL_NAME='tech-data-test1'
nohup python imagenet_100w.py  -a efficientnet_b7 --dist-url tcp://127.0.0.1:12345 --dist-backend nccl --multiprocessing-distributed --world-size 1 --rank 0 /mnt/cfs/chubaofs_tech_data-test/sangqingyuan1/imagenet --epoch 50 -b 1024 >100w_cube_torch.log 2>&1 &
