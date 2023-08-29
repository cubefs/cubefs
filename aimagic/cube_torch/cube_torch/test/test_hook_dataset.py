import os

import torch
import torch.multiprocessing as mp
import torchtext
from torch.utils.data import ConcatDataset
from torchvision import datasets, transforms
import cube_torch

os.environ["CubeFS_ROOT_DIR"] = "/home/guowl/testdata"
os.environ['localIP'] = "127.0.0.1"
os.environ['CubeFS_QUEUE_SIZE_ON_WORKER']='10'

def start_worker_test_concatDataset(i):
    traindir = "/home/guowl/testdata"
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
        train_dataset, batch_size=215, shuffle=True,
        num_workers=5)
    epoch = 0
    while True:
        print("start epoch {} read data".format(epoch))
        for i, t in enumerate(train_loader):
            print("i is {}, epoch {} ".format(i, epoch))
        epoch += 1


def start_worker_test_Dataset(i):
    normalize = transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                     std=[0.229, 0.224, 0.225])

    traindir = "/home/guowl/testdata/data0"
    train_dataset = datasets.ImageFolder(
        traindir,
        transforms.Compose([
            transforms.RandomResizedCrop(224),
            transforms.RandomHorizontalFlip(),
            transforms.ToTensor(),
            normalize,
        ]))

    train_loader = torch.utils.data.DataLoader(
        train_dataset, batch_size=215, shuffle=True,
        num_workers=5)
    epoch = 0
    while True:
        print("start epoch {} read data".format(epoch))
        for i, t in enumerate(train_loader):
            print("i is {}, epoch {}  ".format(i, epoch))
        epoch += 1


if __name__ == '__main__':
    mp.spawn(fn=start_worker_test_concatDataset, nprocs=1)
