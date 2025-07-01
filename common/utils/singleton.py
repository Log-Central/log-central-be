from abc import ABCMeta


class Singleton(ABCMeta):
    """
    ABCMeta를 상속해, 추상 베이스 클래스 기능과
    한 번만 인스턴스 생성하는 Singleton 기능을 모두 갖춘 메타클래스.
    """

    _instances: dict[type, object] = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]
