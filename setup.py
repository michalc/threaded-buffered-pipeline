import setuptools


def long_description():
    with open('README.md', 'r') as file:
        return file.read()


setuptools.setup(
    name='threaded-buffered-pipeline',
    version='0.0.6',
    author='Michal Charemza',
    author_email='michal@charemza.name',
    description='Parallelize pipelines of Python iterables',
    long_description=long_description(),
    long_description_content_type='text/markdown',
    url='https://github.com/michalc/threaded-buffered-pipeline',
    py_modules=[
        'threaded_buffered_pipeline',
    ],
    python_requires='>=3.7.1',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
