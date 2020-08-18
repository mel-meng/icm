try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'infoworks',
    'author': 'Mel Meng',
    'url': 'https://bitbucket.org/yuanhangmeng/infoworks/overview',
    'download_url': '',
    'author_email': 'mel.meng.pe@gmail.com',
    'version': '0.1',
    'install_requires': ['nose'],
    'packages': ['infoworks'],
    'scripts': [],
    'name': 'infoworks'
}

setup(**config)