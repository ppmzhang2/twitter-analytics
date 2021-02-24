import sys
from typing import Optional, List

from app.saver import Saver

args = sys.argv[1:]

funcs = {
    'reset': (0, Saver, Saver.reset),
    'seeds': (-1, Saver, Saver.seeds),
    'fullauto': (0, Saver, Saver.automaton),
    'export': (0, Saver, Saver.export)
}


def request() -> Optional[str]:
    """get the input request string, which will be then mapped as key to get
    main method

    :return:
    """
    try:
        return str(args[0])
    except IndexError:
        return None


def arg1() -> Optional[int]:
    """get the 1sr argument to requested function

    :return:
    """
    try:
        return int(args[1])
    except (IndexError, ValueError):
        return None


def args_int() -> Optional[List[int]]:
    """get all arguments as seed user IDs

    :return:
    """
    try:
        return [int(i) for i in args[1:]]
    except (IndexError, ValueError):
        return None


def main() -> None:
    if request() is None:
        raise TypeError('expect at least one input')

    n_args, cls, func = funcs.get(request(), (None, None, None))

    if func is None:
        raise TypeError('input request is not valid, accept only {}'.format(
            list(funcs.keys())))

    if n_args == 0:
        instance = cls()
        func(instance)
    elif n_args == -1 and args_int() is None:
        raise ValueError('User IDs must ALL be integers')
    elif n_args == -1:
        instance = cls()
        func(instance, *args_int())
    elif n_args == 1 and arg1() is None:
        raise TypeError('user ID not provided')
    elif n_args == 1:
        instance = cls()
        func(instance, arg1())
    else:
        raise TypeError('input error')


if __name__ == '__main__':
    main()
