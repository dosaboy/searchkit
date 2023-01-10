from setuptools import setup, find_packages


def get_dependencies():
    """Reads the dependencies from the requirements file."""
    with open('requirements.txt', 'r') as d:
        dependencies = d.read()

    return dependencies


setup(
    name='searchtools',
    version='1.0.0',
    packages=find_packages(include=['searchtools*']),
    install_requires=get_dependencies(),
)
