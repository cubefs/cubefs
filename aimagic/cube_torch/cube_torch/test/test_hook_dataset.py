import os
import time

import cube_torch
import torch
import torch.multiprocessing as mp
from torch.utils.data import ConcatDataset
from torchvision import datasets, transforms

os.environ["CubeFS_ROOT_DIR"] = "/mnt/cfs/chubaofs_tech_data-test"
os.environ['localIP'] = "127.0.0.1"
os.environ['USE_BATCH_DOWNLOAD'] = '123'


def start_worker_test_concatDataset(i):
    traindir = "/mnt/cfs/chubaofs_tech_data-test"
    normalize = transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                     std=[0.229, 0.224, 0.225])

    traindir_list = [os.path.join(traindir, 'data' + str(i)) for i in range(3)]
    train_datasets = []
    for traindir in traindir_list:
        train_data = datasets.ImageFolder(
            traindir,
            transforms.Compose([
                transforms.RandomResizedCrop(224),
                transforms.RandomHorizontalFlip(),
                transforms.ToTensor(),
                normalize,
            ]))
        train_datasets.append(train_data)
    train_dataset = ConcatDataset(train_datasets)
    train_loader = torch.utils.data.DataLoader(
        train_dataset, batch_size=20, shuffle=True,
        num_workers=5)
    epoch = 0
    while True:
        print("start epoch {} read data".format(epoch))
        for i, t in enumerate(train_loader):
            print("i is {}, epoch {} ".format(i, epoch))
            time.sleep(1)
        epoch += 1


def start_worker_test_Dataset(i):
    normalize = transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                     std=[0.229, 0.224, 0.225])

    traindir = "/mnt/cfs/chubaofs_tech_data-test/data0"
    train_dataset = datasets.ImageFolder(
        traindir,
        transforms.Compose([
            transforms.RandomResizedCrop(224),
            transforms.RandomHorizontalFlip(),
            transforms.ToTensor(),
            normalize,
        ]))

    train_loader = torch.utils.data.DataLoader(
        train_dataset, batch_size=3, shuffle=True,
        num_workers=5)
    epoch = 0
    while True:
        print("start epoch {} read data".format(epoch))
        for i, t in enumerate(train_loader):
            print("i is {}, epoch {}  ".format(i, epoch))
        epoch += 1


if __name__ == '__main__':
    mp.spawn(fn=start_worker_test_concatDataset, nprocs=1)
