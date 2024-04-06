from abc import ABC


class TdeEntry(ABC):
    """
    Abstract class representing an Entry, need to be implemented and inherited to create a wrapper
    """

    def __init__(self):
        pass


# # A typical Emergency Department Entry
class EDEntry(TdeEntry):

    def __init__(self):
        super().__init__()

        # there are a few field that should be present for any entry

        # should be a string (hashable type)
        self.id = None

        # Should be in datetime format
        self.arrival_date = None

        # Should be in datetime format
        self.departure_date = None

        self.gender = None

    @property
    def is_girl(self):
        if self.gender is None:
            return True
        return self.gender == "F"
