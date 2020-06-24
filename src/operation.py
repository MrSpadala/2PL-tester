

class Operation:
    """
    Class describing an operation of the schedule
    """
    def __init__(self, type, trans, obj):
        if type==None or trans==None or obj==None:
            raise ValueError

        self.type = str(type)  #can be either READ, WRITE, UNLOCKED, SHARED_L, XCLUSIVE_L
        self.transaction = str(trans)  #identifier of the transaction executing this operation
        self.obj = str(obj)  #object on which this operation is executed

        # Boolean telling if this operation is the last performed by transaction 'trans'
        # It is False if it is the last one, True otherwise. It will be set accordingly by 'parse_schedule'
        self.tx_continues = True


    def __str__(self):
        if self.type == 'READ':
            s = 'r'
        elif self.type == 'WRITE':
            s = 'w'
        elif self.type == 'UNLOCKED':
            s='u'
        elif self.type == 'SHARED_L':
            s='sl'
        elif self.type == 'XCLUSIVE_L':
            s='xl'
        else:
            print('WARNING: op type not recognized')
            s = self.type
        return s+self.transaction+'('+self.obj+')'

    def __repr__(self):  #debugging
        return self.__str__()

