class PickyDefaultDict(dict):
    """A ``defaultdict`` alternative that requires values of a specific type.
    
    Default values are constructed with no arguments by default;
    supply ``args_munger`` and/or ``kwargs_munger`` to override this.
    They take arguments ``self`` and the unused key being looked up.
    
    """
    def __init__(self, type=object, args_munger=lambda self, k: tuple(), kwargs_munger=lambda self, k: dict()):
        self.type = type
        self.args_munger = args_munger
        self.kwargs_munger = kwargs_munger

    def __getitem__(self, k):
        if k in self:
            return super(PickyDefaultDict, self).__getitem__(k)
        ret = self[k] = self.type(*self.args_munger(self, k), **self.kwargs_munger(self, k))
        return ret

    def __setitem__(self, k, v):
        if not isinstance(v, self.type):
            raise TypeError("Expected {}, got {}".format(self.type, type(v)))
        super(PickyDefaultDict, self).__setitem__(k, v)


class StructuredDefaultDict(dict):
    """A ``defaultdict``-like class that expects values stored at a specific depth.
    
    Requires an integer to tell it how many layers deep to go.
    The innermost layer will be ``PickyDefaultDict``, which will take the
    ``type``, ``args_munger``, and ``kwargs_munger`` arguments supplied
    to my constructor.
    
    This will never accept manual assignments at any layer but the deepest.
    
    """
    def __init__(self, layers, type=object, args_munger=lambda self, k: tuple(), kwargs_munger=lambda self, k: dict()):
        if layers < 1:
            raise ValueError("Not enough layers")
        self.layer = layers
        self.type = type
        self.args_munger = args_munger
        self.kwargs_munger = kwargs_munger

    def __getitem__(self, k):
        if k in self:
            return super(StructuredDefaultDict, self).__getitem__(k)
        if self.layer < 2:
            ret = PickyDefaultDict(self.type, self.args_munger, self.kwargs_munger)
        else:
            ret = StructuredDefaultDict(self.layer-1, self.type, self.args_munger, self.kwargs_munger)
        super(StructuredDefaultDict, self).__setitem__(k, ret)
        return ret

    def __setitem__(self, k, v):
        raise TypeError("Can't set layer {}".format(self.layer))
