
from queue import SimpleQueue
from collections import defaultdict
from utils import parse_schedule, _sched_malformed_err



DEBUG = True
def debug(*args):
	msg = ''
	for arg in args:
		msg += str(arg)+' '
	if DEBUG:	print(msg)



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
		if len(list(negative_ts)) > 0:
			return _sched_malformed_err('Transactions (their timestamps) must be non negative')
	except ValueError:
		return _sched_malformed_err('Transactions (their timestamps) must be integers')


	# timestamps information for each object
	data_entry = [-1] * 4  #dummy entry, will initialize all entries like this
	# indices of the data_entry array where the timestamp information is stored
	RTS, WTS, WTS_C, CB = 0, 1, 2, 3
	data_entry[CB] = 1  #commit bit initialized with 1
	timestamps_data = {op.obj: data_entry.copy() for op in schedule}


	# save objects written by each transaction
	written_obj = defaultdict(set)   #dict[transaction] = written objects by transaction

	# save transactions waiting for object release
	tx_waiting_for_obj = defaultdict(set)   #dict[object] = transaction waiting for object release (e.g. its commit bit)

	# set of waiting transactions
	waiting_tx = set()


	# final solution
	solution = list()


	# - - - - utils - - -

	def commit(tx):
		"""Performs the commit of transaction 'tx'
		"""
		#print('Committing', tx)
		solution.append('commit '+str(tx))
		for obj in written_obj[tx]:
			data = timestamps_data[obj]
			data[CB] = True
			# release transactions
			waiting_tx -= tx_waiting_for_obj[obj]
			tx_waiting_for_obj[obj].clear()

	def rollback(tx):
		"""Performs the rollback of transaction 'tx'
		"""
		#print('Rollback', tx)
		solution.append('rollback '+str(tx))
		for obj in written_obj[tx]:
			data = timestamps_data[obj]
			data[WTS] = data[WTS_C]
			data[CB] = True
			# release transactions
			waiting_tx -= tx_waiting_for_obj[obj]
			tx_waiting_for_obj[obj].clear()

	def execute(op):
		"""Execute operation op. Write in the solution its execution and,
		whether it is the last, the commit of its transaction
		"""
		solution.append(str(operation))
		if not operation.tx_continues:
			commit(operation.transaction)





	# - - - - main - - -

	# save operations in queue
	waiting_ops = defaultdict(list)  #dict[transaction] = waiting operations of that transaction

	i = 0
	while True:

		# Check if there is some waiting operation in the queue, if so execute it first
		released_transactions = set(waiting_ops.keys()) - waiting_tx
		debug('released transactions:', released_transactions)
		if len(released_transactions) > 0:
			tx = released_transactions.pop()

			operation = waiting_ops[tx].pop(0)

			debug('Found operation released', operation)

			if len(waiting_ops[tx]) <= 0:
				del waiting_ops[tx]
		
		else:  # no waiting operation, go on with the schedule

			if i >= len(schedule):
				# If here, there are no waiting operation and the schedule is empty, can return solution
				return solution

			operation = schedule[i]
			debug('Picked operation from schedule', operation)
			# Check if transaction of the operation is in wait, if so put operation in queue
			tx = operation.transaction
			if tx in waiting_tx:
				debug('Sending operation in queue', operation)
				waiting_ops[tx].append(operation)
				i += 1
				continue

		debug('executing operation', operation)

		transaction = operation.transaction
		obj = operation.obj
		ts_obj = timestamps_data[obj]

		if operation.type == 'READ':
			if TS(transaction) >= ts_obj[WTS]:
				if ts_obj[CB] or TS(transaction) == ts_obj[WTS]:
					ts_obj[RTS] = max(TS(transaction), ts_obj[RTS])
					execute(operation)
				else:
					debug('put operation in wait', operation)
					waiting_tx.add(transaction)
					tx_waiting_for_obj[obj].add(transaction)
			else:
				rollback(transaction)


		elif operation.type == 'WRITE':
			if TS(transaction) >= ts_obj[RTS] and TS(transaction) >= ts_obj[WTS]:
				if ts_obj[CB]:
					ts_obj[WTS] = TS(transaction)
					ts_obj[CB] = False
					execute(operation)
				else:
					waiting_tx.add(transaction)
					tx_waiting_for_obj[obj].add(transaction)
					solution.append('wait '+str(operation))
			else:
				if TS(transaction) >= ts_obj[RTS] and TS(transaction) < ts_obj[WTS]:
					if ts_obj[CB]:
						solution.append('operation ignored '+str(operation))
					else:
						waiting_tx.add(transaction)
						tx_waiting_for_obj[obj].add(transaction)
						solution.append('wait '+str(operation))
				else:
					rollback(transaction)
			

		else:
			raise ValueError('Bad operation type')

		i += 1



if __name__ == '__main__':
	schedule = parse_schedule('w1(x)r2(x)w1(y)')
	#schedule = parse_schedule('')
	solution = solveTimestamps(schedule)
	from pprint import pprint
	pprint(solution)


