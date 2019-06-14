
from collections import defaultdict
from utils import parse_schedule, _sched_malformed_err


from os.path import isfile
DEBUG = isfile('.DEBUG')
def debug(*args):
	msg = ''
	for arg in args:
		msg += str(arg)+' '
	if DEBUG:	print(msg)




def solveTimestamps(schedule):
	"""Returns true of false whether the schedule is serializable through timestamps
	"""
	# timestamps information for each object
	data_entry = [-1] * 4  #dummy entry, will initialize all entries like this
	# indices of the data_entry array where the timestamp information is stored
	RTS, WTS, WTS_C, CB = 0, 1, 2, 3
	data_entry[CB] = True  #commit bit initialized to 1
	timestamps_data = {op.obj: data_entry.copy() for op in schedule}


	# save objects written by each transaction
	written_obj = defaultdict(set)   #dict[transaction] = written objects by transaction

	# set of waiting transactions
	waiting_tx = {}  
	# if 'T' is a waiting transaction, then waiting_tx['T'] is the index of the operation of 'T' in the schedule where 'T' is blocked


	# final solution
	solution = list()



	# - - - - utils - - -

	def commit(tx):
		"""Performs the commit of transaction 'tx'
		"""
		solution.append('commit '+str(tx))
		debug('committing', tx)
		for obj in written_obj[tx]:
			data = timestamps_data[obj]
			data[WTS_C] = data[WTS]
			data[CB] = True

	def rollback(tx):
		"""Performs the rollback of transaction 'tx'
		"""
		solution.append('rollback '+str(tx))
		debug('rollbacking', tx)
		for obj in written_obj[tx]:
			data = timestamps_data[obj]
			data[WTS] = data[WTS_C]
			data[CB] = True

	def execute(op):
		"""Execute operation op. Write in the solution its execution and,
		whether it is the last, the commit of its transaction
		"""
		solution.append(str(operation))
		if op.type == 'WRITE':
			written_obj[op.transaction].add(op.obj)
		if not operation.tx_continues:
			commit(operation.transaction)


	def TS(tx):
		"""Returns the timestamp of a transaction.
		we ASSUME that timestamp of transaction 'i' is 'i'
		"""
		return int(tx)

	# sanity check on the timestamps
	try:
		err = None
		all_timestamps = [TS(op.transaction) for op in schedule]
		negative_ts = filter(lambda x: x<0, all_timestamps)
		if len(list(negative_ts)) > 0:
			err = _sched_malformed_err('Transactions (their timestamps) must be non negative')
	except ValueError:
		err = _sched_malformed_err('Transactions (their timestamps) must be integers')
	finally:
		if err:	return {'err': err}




	# - - - - main - - -

	i = 0
	while True:

		debug('\nNEW STEP')

		# Fetch new operation

		# First try to fetch the next operation if a transaction has been unlocked
		locking_ops_index = sorted(waiting_tx.values())
		for j in locking_ops_index:
			obj = schedule[j].obj
			if timestamps_data[obj][CB] == True:
				operation = schedule[j]
				waiting_tx.pop(operation.transaction)
				break

		else:  # no waiting operation, go on with the schedule
			if i >= len(schedule):
				# If here, there are no waiting operation and the schedule is empty, can return solution
				#return solution
				return {'err': None, 'sol': solution, 'waiting_tx': waiting_tx}

			operation = schedule[i]
			debug('Picked operation from schedule', operation)
			# Check if transaction of the operation is in wait, if so put operation in queue
			tx = operation.transaction
			if tx in waiting_tx:
				debug('Transaction is in waiting', operation)
				i += 1
				continue


		# Now we have fetched the next operation, execute it
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
					waiting_tx[transaction] = i
					solution.append('wait '+str(operation))
			else:
				rollback(transaction)


		elif operation.type == 'WRITE':
			if TS(transaction) >= ts_obj[RTS] and TS(transaction) >= ts_obj[WTS]:
				if ts_obj[CB]:
					ts_obj[WTS] = TS(transaction)
					ts_obj[CB] = False
					execute(operation)
				else:
					debug('put operation in wait', operation)
					waiting_tx[transaction] = i
					solution.append('wait '+str(operation))
			else:
				if TS(transaction) >= ts_obj[RTS] and TS(transaction) < ts_obj[WTS]:
					if ts_obj[CB]:
						solution.append('operation ignored '+str(operation))
					else:
						debug('put operation in wait', operation)
						waiting_tx[transaction] = i
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


