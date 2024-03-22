import numpy as np
import torchvision
import io
import torch
from PIL import Image

import cv2
image_path = '/home/guowl/222.jpg'

f=open(image_path,'rb')
image_bytes=f.read()
f.close()
image = cv2.imdecode(np.frombuffer(image_bytes, np.uint8), -1)

image_new=cv2.imread(image_path,-1)
print("image_new type :{}".format(type(image_new)))
print(np.array_equal(image,image_new))


