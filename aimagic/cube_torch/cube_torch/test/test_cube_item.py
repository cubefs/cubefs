

import multiprocessing

from cube_torch.cube_batch_download import CubeBatchDownloader

downloader=CubeBatchDownloader("http://127.0.0.1")
downloader.batch_download_async()