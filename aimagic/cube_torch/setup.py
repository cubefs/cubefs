from setuptools import setup

setup(
    name='cube_torch',
    version='0.2',
    description='cube_torch is A Pytorch Acceleration Package',
    author='guoweilong',
    author_email='542531652@qq.com',
    include_package_data=True,
    install_requires=[
        'torch',
        'requests',
        'torchvision',
    ],
    packages=['cube_torch'],
    package_data={
        'cube_torch': [

            '__init__.py',
            'test/*.py',
            'example/*.py',
            'mem_manager.py'
            'cube_batch_download.py',
            'cube_loader.py',
            'cube_file.py',
            'cube_worker.py',
            'cube_dataset_info.py',
            'NOTICE',
            'cube_push_data_set_info.py',
            'cube_file_open_interceptor.py',
        ]
    }
)
