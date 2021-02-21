from setuptools import setup

# to install run: python setup.py install

setup(
    name="trio-util",
    version="0.0.01",
    author="Ross Rochford",
    author_email="rochford.ross@gmail.com",
    packages=['trio_util', 'trio_util.pqueue_workers', 'trio_util.pynng'],
    classifiers=[],
)
