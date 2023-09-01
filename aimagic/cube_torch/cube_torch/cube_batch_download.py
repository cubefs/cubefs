import json
import os

import requests

from cube_torch import get_manager


class CubeDownloadItem:

    def __init__(self,file_size,file_content,file_path):
        self._file_size=file_size
        self._file_content=file_content
        self._file_path=file_path

    def get_file_content(self):
        return self._file_content




class CubeBatchDownloader:
    def __init__(self, url):
        self.batch_download_addr = url
        manager = get_manager()
        self.cube_content_cache = manager.dict()
        manager.cube_batch_downloader=self

    def parse_content(self,content):
        version = int.from_bytes(content[:8], byteorder='big')
        print(f'Version: {version}')
        path_count = int.from_bytes(content[8:16], byteorder='big')
        print(f'Path count: {path_count}')
        start = 16
        for i in range(path_count):
            path_size = int.from_bytes(content[start:start+8], byteorder='big')
            start+=8
            path = content[start:start + path_size].decode()
            start += path_size
            content_size = int.from_bytes(content[start:start + 8], byteorder='big')
            start += 8
            if content_size > 0:
                file_content = content[start:start + content_size]
                start += content_size
                cube_content_item=CubeDownloadItem(content_size,file_content,path)
                self.cube_content_cache[path] = cube_content_item
            else:
                print('Content is empty')

    def encode_by_paths(self,path_list):
        content = b''
        content += int(1).to_bytes(8, byteorder='big')
        path_count=len(path_list)
        content += path_count.to_bytes(8, byteorder='big')
        for path in path_list:
            with open(path, 'rb') as f:
                file_content = f.read()
            path_bytes = path.encode()
            content += len(path_bytes).to_bytes(8, byteorder='big')
            content += path_bytes
            content += len(file_content).to_bytes(8, byteorder='big')
            content += file_content
        return content

    def batch_download(self, index_list):
        try:
            data = json.dumps(index_list)
            response = requests.post(self.batch_download_addr, data=data)
            content = response.content
            self.parse_content(content)

        except Exception as e:
            print('Error:', e)
            pass

    def get_path_content(self, path):
        if self.cube_content_cache.get(path) is None:
            return None
        return self.cube_content_cache.pop(path)


if __name__ == '__main__':
    cube_downloader=CubeBatchDownloader("http://127.0.0.1")
    data_dir='/home/guowl/testdata/data0/n01440764/'
    jpg_files=[]
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith('.JPEG'):
                file_path = os.path.join(root, file)
                jpg_files.append(file_path)
                if len(jpg_files) >= 30:
                    break
        if len(jpg_files) >= 30:
            break
    content=cube_downloader.encode_by_paths(jpg_files)
    cube_downloader.parse_content(content)

    for path in jpg_files:
        with open(path, 'rb') as f:
            file_content = f.read()
            cube_content_item = cube_downloader.get_path_content(path)
            assert file_content==cube_content_item['file_content']
            assert path==cube_content_item['file_path']
            assert len(file_content)==cube_content_item['file_size']
            print("file_path:{} file_size:{} file_content is same".format(path,len(file_content)))

