import numpy as np
import torchvision
import io
import torch
from PIL import Image

image_path = '/home/guowl/222.jpg'
tensor2 = torchvision.io.read_image(image_path)
print('torchvision.io.read_image result is {}'.format(tensor2))


def read_image_from_bytes(image_bytes, mode='RGB'):
    fp = io.BytesIO(image_bytes)
    img = Image.open(fp)
    np_img = np.array(img)
    tensor = torch.from_numpy(np_img)
    tensor = tensor.permute((2, 0, 1))
    return tensor


with open(image_path, 'rb') as f:
    data = f.read()

image_bytes = data
tensor1 = read_image_from_bytes(image_bytes)

print('read_image_from_bytes result is {}'.format(tensor1))
assert torch.equal(tensor1, tensor2)
