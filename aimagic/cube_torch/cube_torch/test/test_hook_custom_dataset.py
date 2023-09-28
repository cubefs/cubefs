import os
import time

import torch
from PIL import Image
from torch import multiprocessing
from torch.utils.data import ConcatDataset, Dataset

os.environ["CubeFS_ROOT_DIR"] = "/mnt/cfs/chubaofs_tech_data-test"
os.environ['localIP'] = "127.0.0.1"
# os.environ['USE_BATCH_DOWNLOAD'] = 'true'


class CustomDataSet(Dataset):
    def __init__(self):
        imglist = "0_0_1w.txt"
        titlelist = "0_0_title_1w.txt"
        self.imglist = self.read_file(imglist)
        self.titlelist = self.read_file(titlelist)

    def train_data_list(self):
        return [self.imglist, self.titlelist]

    def read_file(self, filename):
        with open(filename, 'r') as f:
            lines = f.readlines()
        return [line.strip() for line in lines]

    def __len__(self):
        return len(self.imglist)

    def __getitem__(self, index):
        with open(self.imglist[index], "rb") as f:
            img = Image.open(f)
            img.convert("RGB")

        t = torch.load(self.titlelist[index])
        return t


def start_worker_test_concatDataset(i):
    datasets = []
    for i in range(4):
        datasets.append(CustomDataSet())

    train_dataset = ConcatDataset(datasets)

    train_loader = torch.utils.data.DataLoader(
        train_dataset, batch_size=215, shuffle=True,
        num_workers=5)
    epoch = 0
    while True:
        print("start epoch {} read data".format(epoch))
        for i, t in enumerate(train_loader):
            print("i is {}, epoch {} ".format(i, epoch))
            time.sleep(1)
        epoch += 1


def start_worker_test_Dataset(i):
    train_dataset = CustomDataSet()
    train_loader = torch.utils.data.DataLoader(
        train_dataset, batch_size=200, shuffle=True,
        num_workers=5)
    epoch = 0
    while True:
        print("start epoch {} read data".format(epoch))
        for i, t in enumerate(train_loader):
            print("i is {}, epoch {} ".format(i, epoch))
        epoch += 1


if __name__ == '__main__':
    w = multiprocessing.Process(target=start_worker_test_Dataset, args=(1,))
    w.daemon = False
    w.start()
