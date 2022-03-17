"""project init"""
import os

from . import config

env = os.getenv('ENV', 'test')

if env == 'prod':
    cfg = config.Config
else:
    cfg = config.TestConfig

cfg.configure_logger(__name__)
