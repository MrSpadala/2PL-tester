
from __future__ import print_function  #compatibility python2
from operation import Operation

def parse_schedule(sched):
    """
    Parses the input schedule as a string.
    Returns the list of 'Operation' objects
    """
    if sched == '':
        print('Using test schedule')
        return parse_schedule(TEST_SCHEDULES[-1])

    schedule = []

    i = 0
    last_ops = dict()  #save last operation for each transaction

    try:
        while i < len(sched):
            t, tx, o = None, None, None 

            # get operation type 't'
            if sched[i] == 'r':
                t = 'READ'
            elif sched[i] == 'w':
                t = 'WRITE'
            else:
                return _sched_malformed_err('operation types must be \'r\' or \'w\'')
            i += 1

            # get operation transaction 'tx'
            tx_end = sched[i:].find('(')
            tx = sched[i:i+tx_end]
            if tx == '':
                return _sched_malformed_err()
            i = i+tx_end+1
            
            # get operation object 'o'
            o_end = sched[i:].find(')')
            o = sched[i:i+o_end]
            if o == '':
                return _sched_malformed_err()
            i = i+o_end+1


            operation = Operation(t,tx,o)  #operation object

            last_ops[tx] = operation    #save operation as last for transaction tx
            schedule.append(operation)  #append operation to schedule


        # Set final operations for each transaction
        for op in last_ops.values():
            op.tx_continues = False

        return schedule


    except ValueError:
        return _sched_malformed_err()


def _sched_malformed_err(msg=None):
    """
    Returns an error message if the schedule is malformed
    """
    msg = msg if msg else 'schedule malformed'
    help_msg = "<br><br>Enter a schedule like <i>r1(x)w1(y)r2(y)r1(z)</i>"
    return msg+help_msg





def format_schedule(sched):
    """
    Formats a schedule (as list of Operations) for printing in HTML
    """
    s = ''
    for op in sched:
        if op.type!='READ' and op.type!='WRITE':
            s += '<b>'+str(op)+' </b>'
        else:
            s += str(op)+' '
    return s+'\n'



# test schedules
TEST_SCHEDULES = [
    'r1(x)r2(z)r1(z)r3(x)r3(y)w1(x)w3(y)r2(y)w4(z)w2(y)',
    'r1(A)r2(A)r2(B)w1(A)w2(D)r3(C)r1(C)w3(B)r4(A)',
    'r1(A)r2(A)r3(B)w1(A)r2(C)r2(B)w2(B)w1(C)',
    'r1(A)r2(B)r3(B)w1(A)r4(A)r2(C)r2(B)w2(B)w1(C)',
    'w1(x)r2(x)w1(y)',
    'r1(A)r2(A)r3(B)w1(A)r3(A)r2(C)r2(B)w2(B)w1(C)',
    'r1(x)w2(x)r3(x)r1(y)r4(z)w2(y)r1(v)w3(v)r4(v)w4(y)w5(y)w5(z)',
    'r1(A)r2(B)r3(C)r1(B)r2(C)r3(D)w1(C)w2(D)w3(E)',
    'r1(A)r2(B)r3(C)r1(B)r2(C)r3(D)w1(A)w2(B)w3(C)',
    'r1(A)r2(B)r3(C)r1(B)r2(C)r3(A)w1(A)w2(B)w3(C)',
    'r1(A)r2(B)r3(C)w1(B)w2(C)w3(A)',
    'r6(A)r8(A)r9(A)w8(A)w11(A)r10(A)'
]


