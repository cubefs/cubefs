import os
import time
import cube_torch
import torch
from torch import multiprocessing
from torch.utils.data import ConcatDataset, Dataset
from torchvision import transforms

os.environ["CubeFS_ROOT_DIR"] = "/home/guowl/testdata"
os.environ['CubeFS_QUEUE_SIZE_ON_WORKER'] = '1222321321'
os.environ['localIP'] = "127.0.0.1"




class CustomDataSet(Dataset):
    def __init__(self):
        super().__init__()
        imglist = "0_1_10000.txt"
        titlelist = "0_2_10000.txt"
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
        return self.imglist[index], self.titlelist[index]


def start_worker_test_concatDataset(i):
    datasets = []
    for i in range(4):
        datasets.append(CustomDataSet())

    train_dataset = ConcatDataset(datasets)

    train_loader = torch.utils.data.DataLoader(
        train_dataset, batch_size=215, shuffle=True,
        num_workers=2)
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
    train_dataset = CustomDataSet()

    train_loader = torch.utils.data.DataLoader(
        train_dataset, batch_size=215, shuffle=True,
        num_workers=2)
    epoch = 0
    while True:
        print("start epoch {} read data".format(epoch))
        for i, t in enumerate(train_loader):
            print("i is {}, epoch {} ".format(i, epoch))
            time.sleep(1)
        epoch += 1


if __name__ == '__main__':
    w = multiprocessing.Process(target=start_worker_test_Dataset, args=(1,))
    w.daemon = False
    w.start()
