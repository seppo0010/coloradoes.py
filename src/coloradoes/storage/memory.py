class Storage(object):
    def __init__(self):
        super(Storage, self).__init__()
        self.keys = {}

    def set(self, key, value):
        self.keys[key] = str(value)

    def get(self, key):
        return self.keys[key] if key in self.keys else None

    def exists(self, key):
        return key in self.keys

    def delete(self, key):
        if key not in self.keys:
            return False
        del self.keys[key]
        return True
