import math
import os
import time
from datetime import datetime
from functools import reduce
from multiprocessing import Pool as ProcessPool
from typing import List

import torch
from torch.utils.data import Dataset
from torchvision import transforms
from transformers import BertTokenizer
import numpy as np
import utils

INPUT_SIZE = 224

std_transform = transforms.Compose([
    transforms.RandomResizedCrop(INPUT_SIZE),
    transforms.RandomHorizontalFlip(),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])  # ImageNet Norm
])


def read_img_meta(filename):
    with open(filename, 'r') as f:
        lines = f.readlines()
        data = [line.strip() for line in lines]
        return data


def concat_list(a, b):
    return a + b


def prob_mask_like(t, prob):
    return torch.zeros_like(t).float().uniform_(0, 1) < prob


def mask_with_tokens(t, token_ids):
    init_no_mask = torch.full_like(t, False, dtype=torch.bool)
    mask = reduce(lambda acc, el: acc | (t == el), token_ids, init_no_mask)
    return mask


def get_mask_subset_with_prob(mask, prob):
    batch, seq_len, device = *mask.shape, mask.device

    max_masked = math.ceil(prob * seq_len)
    num_tokens = mask.sum(dim=-1, keepdim=True)
    mask_excess = (mask.cumsum(dim=-1) > (num_tokens * prob).ceil())
    mask_excess = mask_excess[:, :max_masked]

    rand = torch.rand((batch, seq_len), device=device).masked_fill(~mask, -1e9)
    _, sampled_indices = rand.topk(max_masked, dim=-1)
    sampled_indices = (sampled_indices + 1).masked_fill_(mask_excess, 0)

    new_mask = torch.zeros((batch, seq_len + 1), device=device)
    new_mask.scatter_(-1, sampled_indices, 1)
    return new_mask[:, 1:].bool()


def gen_mask(seq, mask_token_id, mask_ignore_token_ids, pad_token_id, mask_prob, random_token_prob, replace_prob,
             num_tokens):
    # do not mask [pad] tokens, or any other tokens in the tokens designated to be excluded ([cls], [sep])
    # also do not include these special tokens in the tokens chosen at random

    no_mask = mask_with_tokens(seq, mask_ignore_token_ids)
    mask = get_mask_subset_with_prob(~no_mask, mask_prob)

    # mask input with mask tokens with probability of `replace_prob` (keep tokens the same with probability 1 - replace_prob)

    masked_seq = seq.clone().detach()

    # derive labels to predict

    labels = seq.masked_fill(~mask, pad_token_id)

    # if random token probability > 0 for mlm

    if random_token_prob > 0:
        assert num_tokens is not None, 'num_tokens keyword must be supplied when instantiating MLM if using random token replacement'
        random_token_prob = prob_mask_like(seq, random_token_prob)
        random_tokens = torch.randint(0, num_tokens, seq.shape, device=seq.device)
        random_no_mask = mask_with_tokens(random_tokens, mask_ignore_token_ids)
        random_token_prob &= ~random_no_mask
        masked_seq = torch.where(random_token_prob, random_tokens, masked_seq)

        # remove tokens that were substituted randomly from being [mask]ed later
        mask = mask & ~random_token_prob

    # [mask] input

    replace_prob = prob_mask_like(seq, replace_prob)
    masked_seq = masked_seq.masked_fill(mask * replace_prob, mask_token_id)
    return masked_seq, mask


class VLPDataset(Dataset):
    def __init__(self, image_metas: List[str], title_metas: List[str], max_length=50, image_transform=std_transform,
                 masking_func=None, attrs: List[str] = [], tokenizer=None):
        self.image_metas = image_metas

        with ProcessPool() as p:
            img_file_list = p.map(read_img_meta, self.image_metas)
        self.img_list = reduce(concat_list, img_file_list)

        with ProcessPool() as p:
            title_file_list = p.map(read_img_meta, title_metas)
        self.title_list = reduce(concat_list, title_file_list)

        self.img_list = np.array(self.img_list)
        self.title_list = np.array(self.title_list)

        super().__init__()
        self.max_length = max_length
        self.transform = image_transform
        self.masking_func = masking_func

    def train_data_list(self):
        return np.array([self.img_list, self.title_list])

    ##新增train_data_list函数，表示需要告诉cubefs 所需要关注的训练集文件名列表。
    # 注意，该函数可以返回多个，如上：返回训练图片文件列表、该图片对应的标题文件列表
    # self.img_list 必须是一个numpy的一维数组，可能是：[/mnt/cfs/jpg/1.jpg,/mnt/cfs/jpg/2.jpg,/mnt/cfs/jpg/3.jpg,/mnt/cfs/jpg/4.jpg]
    # self.title_list 必须是一个numpy的一维数组，可能是[/mnt/cfs/title/1.title,/mnt/cfs/title/2.title,/mnt/cfs/title/3.title,/mnt/cfs/title/4.title]
    # 注意img_list 里面的文件名和title_list里面的文件名，必须一一对应。

    def __len__(self):
        return len(self.img_list)

    def __getitem__(self, index):
        try:
            x = utils.default_loader(self.img_list[index])
        except Exception as e:
            print("get {} exception{} currentTime{}".format(self.img_list[index].strip(), e,
                                                            datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
            return self.__getitem__((index + 1) % self.__len__())

        try:
            title_tokens = torch.load(self.title_list[index])
        except Exception as e:
            print("get {} exception{} currentTime{}".format(self.title_list[index].strip(), e,
                                                            datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')))
            return self.__getitem__((index + 1) % self.__len__())

        x = self.transform(x)
        for k in title_tokens:
            title_tokens[k] = title_tokens[k].squeeze()[:self.max_length]
        if self.masking_func is not None:
            masked_text, mask = self.masking_func(title_tokens['input_ids'].unsqueeze(0))
            masked_pos = torch.arange(mask.shape[-1]).masked_select(mask == True)
            masked_ids = torch.index_select(title_tokens['input_ids'], -1, masked_pos)
            masked_pos = torch.cat([masked_pos, torch.zeros(self.max_length - len(masked_pos))], -1).type(torch.int64)
            masked_ids = torch.cat([masked_ids, -100 * torch.ones(self.max_length - len(masked_ids))], -1).type(
                torch.int64)  # -100 is ignore_index
            return {
                "image": x,
                "input_ids": title_tokens["input_ids"],
                "token_type_ids": title_tokens["token_type_ids"],
                "attention_mask": title_tokens["attention_mask"],
                "masked_text_ids": masked_text.squeeze(),
                "masked_pos": masked_pos.squeeze(),
                "masked_ids": masked_ids.squeeze()
            }
        else:
            return {
                "image": x,
                "input_ids": title_tokens["input_ids"],
                "token_type_ids": title_tokens["token_type_ids"],
                "attention_mask": title_tokens["attention_mask"]
            }
