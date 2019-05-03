
from utils import _sched_malformed_err

def solveTimestamps(schedule):
	"""Returns true of false whether the schedule is serializable through timestamps
	"""

	# init object set
	objects = set([op.obj for op in schedule])

	# sanity check on the timestamps
	# we ASSUME that timestamp of transaction 'i' is 'i'
	if not all([op.transaction.isnumeric() for op in schedule]):
		_sched_malformed_err('Transactions (their timestamps) must be non negative integers')

	# indices of the data_entry array where the timestamp information is stored
	RTS = 0
	WTS = 1
	WTS_C = 2
	CB = 3

	# all entries object entries will be initialized with this entry
	data_entry = [None] * 4
	data_entry[CB] = 1  #commit bit initialized with 1


	# save timestamps information per each object
	timestamps = {op.obj: data_entry.copy() for op in schedule}






