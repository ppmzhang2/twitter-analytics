"""singleton meta-class"""


class SingletonMeta(type):
    """singleton meta-class"""
    _instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instance
