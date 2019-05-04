
from collections import defaultdict
from utils import _sched_malformed_err

def solveTimestamps(schedule):
	"""Returns true of false whether the schedule is serializable through timestamps
	"""

	# init object set
	#objects = set([op.obj for op in schedule])


	def TS(tx):
		"""Returns the timestamp of a transaction.
		we ASSUME that timestamp of transaction 'i' is 'i'
		"""
		return int(tx)

	# sanity check on the timestamps
	try:
		all_timestamps = [TS(op.transaction) for op in schedule]
		negative_ts = filter(lambda x: x<0, all_timestamps)
		if len(negative_ts) > 0:
			return _sched_malformed_err('Transactions (their timestamps) must be non negative')
	except ValueError:
		return _sched_malformed_err('Transactions (their timestamps) must be integers')


	# timestamps information for each object
	data_entry = [-1] * 4  #dummy entry, will initialize all entries like this
	# indices of the data_entry array where the timestamp information is stored
	RTS, WTS, WTS_C, CB = 0, 1, 2, 3
	data_entry[CB] = 1       #commit bit initialized with 1
	timestamps_data = {op.obj: data_entry.copy() for op in schedule}


	# save objects written by each transaction
	written_obj = defaultdict(list)   #dict[transaction] = written objects by transaction

	# save transactions waiting for object release
	tx_waiting_for_obj = defaultdict(list)   #dict[object] = transaction waiting for object release (e.g. its commit bit)

	# set of waiting transactions
	waiting_tx = set()




	# - - - - utils - - -

	def commit(tx):
		"""Performs the commit of transaction 'tx'
		"""
		for obj in written_obj[tx]:
			data = timestamps_data[obj]
			data[CB] = True
			for tx_released in tx_waiting_for_obj[obj]:
				# TODO let tx_released proceed (?)
				pass

	def rollback(tx):
		"""Performs the rollback of transaction 'tx'
		"""
		for obj in written_obj[tx]:
			data = timestamps_data[obj]
			data[WTS] = data[WTS_C]
			data[CB] = True
			for tx_released in tx_waiting_for_obj[obj]:
				# TODO let tx_released proceed (?)
				pass






	# - - - - main - - -

	for i in range(len(schedule)):
		operation = schedule[i]
		transaction = operation.transaction
		obj = operation.obj

		ts_obj = timestamps_data[obj]


		if operation.type == 'READ':
			if ts_obj[WTS] <= TS(transaction):
				if ts_obj[CB] or TS(transaction) == ts_obj[WTS]:
					ts_obj[RTS] = max(TS(transaction), ts_obj[RTS])
				else:
					waiting_tx.add(transaction)
					tx_waiting_for_obj[obj].append(transaction)
			else:
				rollback(transaction)


		elif operation.type == 'WRITE':
			pass
			

		else:
			raise ValueError('Bad operation type')




