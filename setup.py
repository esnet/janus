from setuptools import setup, find_packages

setup(
    name='Janus',
    version='0.1',
    description='Janus Controller',
    url='https://github.com/esnet/janus',
    author='Ezra Kissel',

    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],

    keywords='dtn rest controller',

    packages=find_packages(),

    install_requires=['flask==2.0.2', 'flask-restplus==0.13.0', 'portainer-api',
                      'tinydb', 'werkzeug==2.0.2', 'cryptography',
                      'psutil', 'virtfs', 'py-cpuinfo', 'numa',
                      'requests', 'flask-httpauth==4.4.0', 'pyyaml']
)
